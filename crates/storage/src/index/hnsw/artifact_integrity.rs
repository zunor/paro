// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Lazy integrity verification for mmap-backed HNSW artifacts.
//!
//! A single page checksum is the wrong physical contract for a multi-hundred
//! megabyte random-access index: verifying it at open faults the whole graph.
//! HNSW therefore protects its immutable payload in 4 KiB chunks. Payload
//! checksums are themselves protected in 4 KiB pages by a compact directory;
//! opening authenticates only the fixed header and that directory. Checksum
//! pages and payload chunks are then authenticated once, immediately before
//! their bytes become typed graph/scan views.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use memmap2::Mmap;
use paro_common::error::{self as paro_error, Result};

const INTEGRITY_MAGIC: u32 = u32::from_le_bytes(*b"HINT");
const INTEGRITY_VERSION: u32 = 2;
const INTEGRITY_HEADER_LEN: usize = 32;
const INTEGRITY_FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"HEND");
const INTEGRITY_FOOTER_LEN: usize = 16;
const INTEGRITY_CHECKSUM_BYTES: usize = std::mem::size_of::<u32>();
const INTEGRITY_CHECKSUM_PAGE_BYTES: usize = 4 * 1024;
pub(crate) const INTEGRITY_CHUNK_BYTES: usize = 4 * 1024;

const CHUNK_UNVERIFIED: u8 = 0;
const CHUNK_VERIFYING: u8 = 1;
const CHUNK_VALID: u8 = 2;
const CHUNK_CORRUPT: u8 = 3;
const CHUNK_STATE_BITS: usize = 2;
const CHUNK_STATES_PER_WORD: usize = u64::BITS as usize / CHUNK_STATE_BITS;
const CHUNK_STATE_MASK: u64 = (1 << CHUNK_STATE_BITS) - 1;

#[derive(Debug)]
struct PackedVerificationStates {
    count: usize,
    words: Box<[AtomicU64]>,
}

impl PackedVerificationStates {
    fn new(count: usize) -> Self {
        let words = (0..count.div_ceil(CHUNK_STATES_PER_WORD))
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { count, words }
    }

    fn word(&self, index: usize) -> Result<(&AtomicU64, usize)> {
        if index >= self.count {
            return Err(paro_error::data_corrupted(
                "HNSW integrity state index exceeds its authenticated domain",
            ));
        }
        let word = self
            .words
            .get(index / CHUNK_STATES_PER_WORD)
            .ok_or_else(|| {
                paro_error::data_corrupted(
                    "HNSW integrity state word is missing from its authenticated domain",
                )
            })?;
        Ok((word, (index % CHUNK_STATES_PER_WORD) * CHUNK_STATE_BITS))
    }

    fn load(&self, index: usize) -> Result<u8> {
        let (word, shift) = self.word(index)?;
        Ok(((word.load(Ordering::Acquire) >> shift) & CHUNK_STATE_MASK) as u8)
    }

    fn compare_exchange(&self, index: usize, current: u8, next: u8) -> Result<bool> {
        debug_assert!(u64::from(current) <= CHUNK_STATE_MASK);
        debug_assert!(u64::from(next) <= CHUNK_STATE_MASK);
        let (word, shift) = self.word(index)?;
        let mask = CHUNK_STATE_MASK << shift;
        let mut observed = word.load(Ordering::Acquire);
        loop {
            if ((observed & mask) >> shift) as u8 != current {
                return Ok(false);
            }
            let updated = (observed & !mask) | (u64::from(next) << shift);
            match word.compare_exchange_weak(observed, updated, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(true),
                Err(actual) => observed = actual,
            }
        }
    }

    fn store(&self, index: usize, next: u8) -> Result<()> {
        debug_assert!(u64::from(next) <= CHUNK_STATE_MASK);
        let (word, shift) = self.word(index)?;
        let mask = CHUNK_STATE_MASK << shift;
        let mut observed = word.load(Ordering::Relaxed);
        loop {
            let updated = (observed & !mask) | (u64::from(next) << shift);
            match word.compare_exchange_weak(
                observed,
                updated,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => observed = actual,
            }
        }
    }

    #[cfg(test)]
    fn count_in_state(&self, expected: u8) -> usize {
        (0..self.count)
            .filter(|&index| self.load(index).ok() == Some(expected))
            .count()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IntegrityDescriptor {
    pub(crate) offset: usize,
    pub(crate) len: usize,
    pub(crate) checksum: u32,
    pub(crate) artifact_len: usize,
}

#[derive(Debug, Clone)]
pub(crate) enum ArtifactIntegrityBacking {
    Bytes(Bytes),
    Mmap {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl ArtifactIntegrityBacking {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Mmap { mmap, offset, len } => &mmap[*offset..*offset + *len],
        }
    }
}

fn integrity_root_checksum(table: &[u8], directory_offset: usize) -> Result<u32> {
    let header = table.get(..INTEGRITY_HEADER_LEN).ok_or_else(|| {
        paro_error::data_corrupted("HNSW integrity table is missing its fixed header")
    })?;
    let directory_and_footer = table.get(directory_offset..).ok_or_else(|| {
        paro_error::data_corrupted("HNSW integrity directory offset exceeds its table")
    })?;
    Ok(crc32c::crc32c_append(
        crc32c::crc32c(header),
        directory_and_footer,
    ))
}

#[derive(Debug)]
pub(crate) struct ArtifactIntegrity {
    backing: ArtifactIntegrityBacking,
    protected_start: usize,
    protected_len: usize,
    checksum_offset: usize,
    checksum_len: usize,
    directory_offset: usize,
    payload_states: PackedVerificationStates,
    checksum_page_states: PackedVerificationStates,
}

impl ArtifactIntegrity {
    pub(crate) fn open(
        backing: ArtifactIntegrityBacking,
        descriptor: IntegrityDescriptor,
    ) -> Result<Arc<Self>> {
        let bytes = backing.as_bytes();
        if descriptor.artifact_len != bytes.len() {
            return Err(paro_error::data_corrupted(format!(
                "HNSW integrity artifact length mismatch: header={}, backing={}",
                descriptor.artifact_len,
                bytes.len()
            )));
        }
        let table_end = descriptor
            .offset
            .checked_add(descriptor.len)
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity table range overflow"))?;
        let table = bytes.get(descriptor.offset..table_end).ok_or_else(|| {
            paro_error::data_corrupted("HNSW integrity table exceeds artifact backing")
        })?;
        if table.len() < INTEGRITY_HEADER_LEN + INTEGRITY_FOOTER_LEN {
            return Err(paro_error::data_corrupted(
                "HNSW integrity table is truncated",
            ));
        }
        let read_u32 = |offset: usize| {
            u32::from_le_bytes(table[offset..offset + 4].try_into().expect("u32 width"))
        };
        let read_u64 = |offset: usize| {
            u64::from_le_bytes(table[offset..offset + 8].try_into().expect("u64 width"))
        };
        let footer_offset = table.len() - INTEGRITY_FOOTER_LEN;
        let directory_count = usize::try_from(read_u64(footer_offset + 8)).map_err(|_| {
            paro_error::data_corrupted("HNSW integrity directory count exceeds usize")
        })?;
        let directory_len = directory_count
            .checked_mul(INTEGRITY_CHECKSUM_BYTES)
            .ok_or_else(|| {
                paro_error::data_corrupted("HNSW integrity directory length overflow")
            })?;
        let directory_local_offset = footer_offset.checked_sub(directory_len).ok_or_else(|| {
            paro_error::data_corrupted("HNSW integrity directory overlaps its header")
        })?;
        if directory_local_offset < INTEGRITY_HEADER_LEN
            || read_u32(footer_offset) != INTEGRITY_FOOTER_MAGIC
            || read_u32(footer_offset + 4) as usize != INTEGRITY_CHECKSUM_PAGE_BYTES
            || integrity_root_checksum(table, directory_local_offset)? != descriptor.checksum
        {
            return Err(paro_error::data_corrupted(
                "HNSW integrity root directory is corrupted",
            ));
        }
        if read_u32(0) != INTEGRITY_MAGIC
            || read_u32(4) != INTEGRITY_VERSION
            || read_u32(8) as usize != INTEGRITY_CHUNK_BYTES
            || read_u32(12) != 0
        {
            return Err(paro_error::data_corrupted(
                "HNSW integrity table header is invalid",
            ));
        }
        let protected_start = usize::try_from(read_u64(16)).map_err(|_| {
            paro_error::data_corrupted("HNSW integrity protected offset exceeds usize")
        })?;
        let protected_len = usize::try_from(read_u64(24)).map_err(|_| {
            paro_error::data_corrupted("HNSW integrity protected length exceeds usize")
        })?;
        let protected_end = protected_start
            .checked_add(protected_len)
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity protected range overflow"))?;
        if protected_end != descriptor.offset {
            return Err(paro_error::data_corrupted(
                "HNSW integrity table does not immediately follow its protected payload",
            ));
        }
        let chunk_count = protected_len.div_ceil(INTEGRITY_CHUNK_BYTES);
        let checksum_len = chunk_count
            .checked_mul(INTEGRITY_CHECKSUM_BYTES)
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity checksum length overflow"))?;
        let expected_directory_count = checksum_len.div_ceil(INTEGRITY_CHECKSUM_PAGE_BYTES);
        let expected_directory_offset = INTEGRITY_HEADER_LEN
            .checked_add(checksum_len)
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity table length overflow"))?;
        let expected_len = expected_directory_offset
            .checked_add(directory_len)
            .and_then(|len| len.checked_add(INTEGRITY_FOOTER_LEN))
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity table length overflow"))?;
        if directory_count != expected_directory_count
            || directory_local_offset != expected_directory_offset
            || table.len() != expected_len
        {
            return Err(paro_error::data_corrupted(format!(
                "HNSW integrity hierarchy mismatch: chunks={chunk_count}, directory={directory_count}, expected_directory={expected_directory_count}, expected_len={expected_len}, got {}",
                table.len()
            )));
        }
        let checksum_offset = descriptor
            .offset
            .checked_add(INTEGRITY_HEADER_LEN)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum offset overflow"))?;
        let directory_offset = descriptor
            .offset
            .checked_add(directory_local_offset)
            .ok_or_else(|| paro_error::data_corrupted("HNSW directory offset overflow"))?;
        Ok(Arc::new(Self {
            backing,
            protected_start,
            protected_len,
            checksum_offset,
            checksum_len,
            directory_offset,
            payload_states: PackedVerificationStates::new(chunk_count),
            checksum_page_states: PackedVerificationStates::new(directory_count),
        }))
    }

    /// Authenticate every payload chunk intersecting an artifact-relative
    /// byte range. Repeated reads pay one acquire atomic load per chunk; the
    /// CRC and page faults happen only for the first reader.
    pub(crate) fn verify_range(&self, offset: usize, len: usize) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity read range overflow"))?;
        let protected_end = self
            .protected_start
            .checked_add(self.protected_len)
            .expect("validated integrity protected range");
        if offset < self.protected_start || end > protected_end {
            return Err(paro_error::data_corrupted(format!(
                "HNSW integrity read range {offset}..{end} exceeds protected payload {}..{protected_end}",
                self.protected_start
            )));
        }
        let first = (offset - self.protected_start) / INTEGRITY_CHUNK_BYTES;
        let last = (end - 1 - self.protected_start) / INTEGRITY_CHUNK_BYTES;
        for chunk in first..=last {
            self.verify_chunk(chunk)?;
        }
        Ok(())
    }

    fn verify_chunk(&self, chunk: usize) -> Result<()> {
        loop {
            match self.payload_states.load(chunk)? {
                CHUNK_VALID => return Ok(()),
                CHUNK_CORRUPT => {
                    return Err(paro_error::data_corrupted(format!(
                        "HNSW artifact payload chunk {chunk} is corrupted"
                    )))
                }
                CHUNK_UNVERIFIED => {
                    if !self.payload_states.compare_exchange(
                        chunk,
                        CHUNK_UNVERIFIED,
                        CHUNK_VERIFYING,
                    )? {
                        continue;
                    }
                    match self.verify_chunk_once(chunk) {
                        Ok(true) => {
                            self.payload_states.store(chunk, CHUNK_VALID)?;
                            return Ok(());
                        }
                        Ok(false) => {
                            self.payload_states.store(chunk, CHUNK_CORRUPT)?;
                            return Err(paro_error::data_corrupted(format!(
                                "HNSW artifact payload chunk {chunk} checksum mismatch"
                            )));
                        }
                        Err(error) => {
                            self.payload_states.store(chunk, CHUNK_CORRUPT)?;
                            return Err(error);
                        }
                    }
                }
                CHUNK_VERIFYING => std::hint::spin_loop(),
                _ => unreachable!("HNSW integrity chunk has an invalid state"),
            }
        }
    }

    fn verify_chunk_once(&self, chunk: usize) -> Result<bool> {
        let relative_start = chunk
            .checked_mul(INTEGRITY_CHUNK_BYTES)
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity chunk offset overflow"))?;
        let start = self
            .protected_start
            .checked_add(relative_start)
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity chunk start overflow"))?;
        let protected_end = self
            .protected_start
            .checked_add(self.protected_len)
            .ok_or_else(|| paro_error::data_corrupted("HNSW protected range overflow"))?;
        let end = start
            .saturating_add(INTEGRITY_CHUNK_BYTES)
            .min(protected_end);
        let payload = self.backing.as_bytes().get(start..end).ok_or_else(|| {
            paro_error::data_corrupted("HNSW integrity chunk exceeds artifact backing")
        })?;
        Ok(crc32c::crc32c(payload) == self.expected_checksum(chunk)?)
    }

    fn verify_checksum_page(&self, page: usize) -> Result<()> {
        loop {
            match self.checksum_page_states.load(page)? {
                CHUNK_VALID => return Ok(()),
                CHUNK_CORRUPT => {
                    return Err(paro_error::data_corrupted(format!(
                        "HNSW integrity checksum page {page} is corrupted"
                    )))
                }
                CHUNK_UNVERIFIED => {
                    if !self.checksum_page_states.compare_exchange(
                        page,
                        CHUNK_UNVERIFIED,
                        CHUNK_VERIFYING,
                    )? {
                        continue;
                    }
                    match self.verify_checksum_page_once(page) {
                        Ok(true) => {
                            self.checksum_page_states.store(page, CHUNK_VALID)?;
                            return Ok(());
                        }
                        Ok(false) => {
                            self.checksum_page_states.store(page, CHUNK_CORRUPT)?;
                            return Err(paro_error::data_corrupted(format!(
                                "HNSW integrity checksum page {page} checksum mismatch"
                            )));
                        }
                        Err(error) => {
                            self.checksum_page_states.store(page, CHUNK_CORRUPT)?;
                            return Err(error);
                        }
                    }
                }
                CHUNK_VERIFYING => std::hint::spin_loop(),
                _ => unreachable!("HNSW checksum page has an invalid state"),
            }
        }
    }

    fn verify_checksum_page_once(&self, page: usize) -> Result<bool> {
        let relative_start = page
            .checked_mul(INTEGRITY_CHECKSUM_PAGE_BYTES)
            .ok_or_else(|| {
                paro_error::data_corrupted("HNSW integrity checksum-page offset overflow")
            })?;
        let start = self
            .checksum_offset
            .checked_add(relative_start)
            .ok_or_else(|| {
                paro_error::data_corrupted("HNSW integrity checksum-page start overflow")
            })?;
        let checksum_end = self
            .checksum_offset
            .checked_add(self.checksum_len)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum range overflow"))?;
        let end = start
            .saturating_add(INTEGRITY_CHECKSUM_PAGE_BYTES)
            .min(checksum_end);
        let checksum_page = self.backing.as_bytes().get(start..end).ok_or_else(|| {
            paro_error::data_corrupted("HNSW integrity checksum page exceeds artifact backing")
        })?;

        let relative_expected = page
            .checked_mul(INTEGRITY_CHECKSUM_BYTES)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum-directory index overflow"))?;
        let expected_start = self
            .directory_offset
            .checked_add(relative_expected)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum-directory offset overflow"))?;
        let expected_end = expected_start
            .checked_add(INTEGRITY_CHECKSUM_BYTES)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum-directory width overflow"))?;
        let expected_raw = self
            .backing
            .as_bytes()
            .get(expected_start..expected_end)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum directory is truncated"))?;
        let expected = u32::from_le_bytes(expected_raw.try_into().map_err(|_| {
            paro_error::data_corrupted("HNSW checksum-directory entry has invalid width")
        })?);
        Ok(crc32c::crc32c(checksum_page) == expected)
    }

    fn expected_checksum(&self, chunk: usize) -> Result<u32> {
        if chunk >= self.payload_states.count {
            return Err(paro_error::data_corrupted(
                "HNSW integrity checksum index exceeds checksum table",
            ));
        }
        let relative = chunk
            .checked_mul(INTEGRITY_CHECKSUM_BYTES)
            .ok_or_else(|| paro_error::data_corrupted("HNSW integrity checksum index overflow"))?;
        self.verify_checksum_page(relative / INTEGRITY_CHECKSUM_PAGE_BYTES)?;
        let start = self
            .checksum_offset
            .checked_add(relative)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum offset overflow"))?;
        let end = start
            .checked_add(INTEGRITY_CHECKSUM_BYTES)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum width overflow"))?;
        let raw = self
            .backing
            .as_bytes()
            .get(start..end)
            .ok_or_else(|| paro_error::data_corrupted("HNSW checksum table is truncated"))?;
        Ok(u32::from_le_bytes(
            raw.try_into().expect("validated checksum width"),
        ))
    }

    #[cfg(test)]
    pub(crate) fn verified_chunk_count(&self) -> usize {
        self.payload_states.count_in_state(CHUNK_VALID)
    }

    #[cfg(test)]
    pub(crate) fn chunk_count(&self) -> usize {
        self.payload_states.count
    }
}

pub(crate) fn append_integrity_table(
    data: &mut Vec<u8>,
    protected_start: usize,
) -> Result<IntegrityDescriptor> {
    if protected_start > data.len() {
        return Err(paro_error::internal(
            "HNSW integrity protected offset exceeds serialized payload",
        ));
    }
    let offset = data.len();
    let protected_len = offset - protected_start;
    let chunk_count = protected_len.div_ceil(INTEGRITY_CHUNK_BYTES);
    let checksum_len = chunk_count
        .checked_mul(INTEGRITY_CHECKSUM_BYTES)
        .ok_or_else(|| paro_error::out_of_range("HNSW checksum table exceeds usize"))?;
    let directory_count = checksum_len.div_ceil(INTEGRITY_CHECKSUM_PAGE_BYTES);
    let directory_len = directory_count
        .checked_mul(INTEGRITY_CHECKSUM_BYTES)
        .ok_or_else(|| paro_error::out_of_range("HNSW integrity directory exceeds usize"))?;
    let table_len = INTEGRITY_HEADER_LEN
        .checked_add(checksum_len)
        .and_then(|len| len.checked_add(directory_len))
        .and_then(|len| len.checked_add(INTEGRITY_FOOTER_LEN))
        .ok_or_else(|| paro_error::out_of_range("HNSW integrity table exceeds usize"))?;
    data.reserve(table_len);
    data.extend_from_slice(&INTEGRITY_MAGIC.to_le_bytes());
    data.extend_from_slice(&INTEGRITY_VERSION.to_le_bytes());
    data.extend_from_slice(&(INTEGRITY_CHUNK_BYTES as u32).to_le_bytes());
    data.extend_from_slice(&0u32.to_le_bytes());
    data.extend_from_slice(
        &u64::try_from(protected_start)
            .map_err(|_| paro_error::out_of_range("HNSW protected offset exceeds u64"))?
            .to_le_bytes(),
    );
    data.extend_from_slice(
        &u64::try_from(protected_len)
            .map_err(|_| paro_error::out_of_range("HNSW protected length exceeds u64"))?
            .to_le_bytes(),
    );
    for chunk in 0..chunk_count {
        let start = protected_start + chunk * INTEGRITY_CHUNK_BYTES;
        let end = start.saturating_add(INTEGRITY_CHUNK_BYTES).min(offset);
        let checksum = crc32c::crc32c(&data[start..end]);
        data.extend_from_slice(&checksum.to_le_bytes());
    }
    let directory_offset = data.len();
    let checksum_offset = offset + INTEGRITY_HEADER_LEN;
    let checksum_end = checksum_offset + checksum_len;
    for page in 0..directory_count {
        let start = checksum_offset + page * INTEGRITY_CHECKSUM_PAGE_BYTES;
        let end = start
            .saturating_add(INTEGRITY_CHECKSUM_PAGE_BYTES)
            .min(checksum_end);
        let checksum = crc32c::crc32c(&data[start..end]);
        data.extend_from_slice(&checksum.to_le_bytes());
    }
    data.extend_from_slice(&INTEGRITY_FOOTER_MAGIC.to_le_bytes());
    data.extend_from_slice(&(INTEGRITY_CHECKSUM_PAGE_BYTES as u32).to_le_bytes());
    data.extend_from_slice(
        &u64::try_from(directory_count)
            .map_err(|_| paro_error::out_of_range("HNSW integrity directory exceeds u64"))?
            .to_le_bytes(),
    );
    let len = data.len() - offset;
    debug_assert_eq!(len, table_len);
    let checksum = integrity_root_checksum(&data[offset..], directory_offset - offset)?;
    Ok(IntegrityDescriptor {
        offset,
        len,
        checksum,
        artifact_len: data.len(),
    })
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn verifies_only_touched_chunks_and_remembers_corruption() {
        let mut data = vec![7_u8; INTEGRITY_CHUNK_BYTES * 3 + 17];
        let descriptor = append_integrity_table(&mut data, 0).unwrap();
        let integrity = ArtifactIntegrity::open(
            ArtifactIntegrityBacking::Bytes(Bytes::from(data.clone())),
            descriptor,
        )
        .unwrap();
        integrity.verify_range(10, 20).unwrap();
        integrity
            .verify_range(INTEGRITY_CHUNK_BYTES * 2, 8)
            .unwrap();
        assert_eq!(integrity.payload_states.load(0).unwrap(), CHUNK_VALID);
        assert_eq!(integrity.payload_states.load(1).unwrap(), CHUNK_UNVERIFIED);
        assert_eq!(integrity.verified_chunk_count(), 2);
        assert_eq!(
            integrity.checksum_page_states.count_in_state(CHUNK_VALID),
            1
        );

        data[INTEGRITY_CHUNK_BYTES + 5] ^= 1;
        let corrupted = ArtifactIntegrity::open(
            ArtifactIntegrityBacking::Bytes(Bytes::from(data)),
            descriptor,
        )
        .unwrap();
        assert!(corrupted.verify_range(INTEGRITY_CHUNK_BYTES, 8).is_err());
        assert!(corrupted.verify_range(INTEGRITY_CHUNK_BYTES, 8).is_err());
        assert_eq!(corrupted.payload_states.load(1).unwrap(), CHUNK_CORRUPT);
    }

    #[test]
    fn open_authenticates_only_the_compact_root_directory() {
        let mut data = vec![3_u8; INTEGRITY_CHUNK_BYTES * 2_049];
        let descriptor = append_integrity_table(&mut data, 0).unwrap();
        let integrity = ArtifactIntegrity::open(
            ArtifactIntegrityBacking::Bytes(Bytes::from(data)),
            descriptor,
        )
        .unwrap();

        assert_eq!(integrity.verified_chunk_count(), 0);
        assert_eq!(
            integrity.checksum_page_states.count_in_state(CHUNK_VALID),
            0
        );
        assert_eq!(
            integrity.payload_states.words.len(),
            2_049_usize.div_ceil(32)
        );
        assert_eq!(integrity.checksum_page_states.count, 3);
    }

    #[test]
    fn checksum_pages_are_authenticated_before_payload_checksums_are_trusted() {
        let mut data = vec![11_u8; INTEGRITY_CHUNK_BYTES * 2];
        let descriptor = append_integrity_table(&mut data, 0).unwrap();
        data[descriptor.offset + INTEGRITY_HEADER_LEN] ^= 1;
        let integrity = ArtifactIntegrity::open(
            ArtifactIntegrityBacking::Bytes(Bytes::from(data)),
            descriptor,
        )
        .unwrap();

        assert!(integrity.verify_range(0, 1).is_err());
        assert_eq!(
            integrity.checksum_page_states.load(0).unwrap(),
            CHUNK_CORRUPT
        );
        assert_eq!(integrity.payload_states.load(0).unwrap(), CHUNK_CORRUPT);
        assert!(integrity.verify_range(0, 1).is_err());
    }

    #[test]
    fn root_directory_corruption_is_rejected_at_open() {
        let mut data = vec![13_u8; INTEGRITY_CHUNK_BYTES * 1_025];
        let descriptor = append_integrity_table(&mut data, 0).unwrap();
        let checksum_len = 1_025 * INTEGRITY_CHECKSUM_BYTES;
        data[descriptor.offset + INTEGRITY_HEADER_LEN + checksum_len] ^= 1;

        assert!(ArtifactIntegrity::open(
            ArtifactIntegrityBacking::Bytes(Bytes::from(data)),
            descriptor,
        )
        .is_err());
    }

    #[test]
    fn packed_states_are_safe_across_chunks_and_concurrent_readers() {
        let chunk_count = 64;
        let mut data = vec![17_u8; INTEGRITY_CHUNK_BYTES * chunk_count];
        let descriptor = append_integrity_table(&mut data, 0).unwrap();
        let integrity = ArtifactIntegrity::open(
            ArtifactIntegrityBacking::Bytes(Bytes::from(data)),
            descriptor,
        )
        .unwrap();

        let threads = (0..chunk_count)
            .map(|chunk| {
                let integrity = Arc::clone(&integrity);
                thread::spawn(move || {
                    for _ in 0..8 {
                        integrity
                            .verify_range(chunk * INTEGRITY_CHUNK_BYTES, 1)
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(integrity.verified_chunk_count(), chunk_count);
        assert_eq!(integrity.payload_states.words.len(), 2);

        let readers = (0..16)
            .map(|_| {
                let integrity = Arc::clone(&integrity);
                thread::spawn(move || integrity.verify_range(0, 1).unwrap())
            })
            .collect::<Vec<_>>();
        for reader in readers {
            reader.join().unwrap();
        }
    }

    #[test]
    fn empty_protected_payload_has_a_valid_root() {
        let mut data = Vec::new();
        let descriptor = append_integrity_table(&mut data, 0).unwrap();
        let integrity = ArtifactIntegrity::open(
            ArtifactIntegrityBacking::Bytes(Bytes::from(data)),
            descriptor,
        )
        .unwrap();

        assert_eq!(integrity.chunk_count(), 0);
        assert_eq!(integrity.checksum_page_states.count, 0);
    }
}
