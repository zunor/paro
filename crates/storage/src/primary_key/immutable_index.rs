// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::primary_key::{PrimaryIndexVersion, PrimaryKeyWriteConflict, RowID};
use paro_common::error::{self as paro_error, Result};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

pub const IMMUTABLE_INDEX_MAGIC: [u8; 4] = *b"PIIX";
pub const IMMUTABLE_INDEX_FORMAT_VERSION: u32 = 2;
pub const DEFAULT_IMMUTABLE_PAGE_SIZE: usize = 4096;
pub const DEFAULT_TARGET_BUCKET_ENTRIES: usize = 64;
pub const DEFAULT_BLOOM_WORDS: usize = 4;

const FILE_HEADER_LEN: usize = 32;
const BUCKET_DIRECTORY_ENTRY_LEN: usize = 8;
const PAGE_HEADER_LEN: usize = 8 + DEFAULT_BLOOM_WORDS * 8;
const PAGE_HEADER_RESERVED: u16 = 0;
const BLOOM_HASH_ROUNDS: usize = 4;

static IMMUTABLE_INDEX_CACHE: LazyLock<Mutex<HashMap<PathBuf, Weak<ImmutableIndexReader>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Copy)]
pub struct ImmutableIndexBuildOptions {
    pub page_size: usize,
    pub target_bucket_entries: usize,
}

impl Default for ImmutableIndexBuildOptions {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_IMMUTABLE_PAGE_SIZE,
            target_bucket_entries: DEFAULT_TARGET_BUCKET_ENTRIES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImmutableIndexStats {
    pub bucket_count: usize,
    pub page_count: usize,
    pub entry_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct BucketDirectoryEntry {
    start_page: u32,
    page_count: u32,
}

#[derive(Debug)]
pub struct ImmutableIndexWriter {
    options: ImmutableIndexBuildOptions,
}

impl Default for ImmutableIndexWriter {
    fn default() -> Self {
        Self::new(ImmutableIndexBuildOptions::default())
    }
}

impl ImmutableIndexWriter {
    pub fn new(options: ImmutableIndexBuildOptions) -> Self {
        Self { options }
    }

    pub fn write_entries(
        &self,
        path: impl AsRef<Path>,
        entries: &[(Vec<u8>, PrimaryIndexVersion)],
    ) -> Result<ImmutableIndexStats> {
        if self.options.page_size <= PAGE_HEADER_LEN {
            return Err(paro_error::invalid_input(
                "immutable index page size is too small",
            ));
        }

        let entries = normalize_entries(entries);
        let bucket_count = choose_bucket_count(entries.len(), self.options.target_bucket_entries);
        let mut buckets = vec![Vec::new(); bucket_count];
        for (key, version) in &entries {
            let bucket = bucket_index_for_key(key, bucket_count);
            buckets[bucket].push((key.clone(), *version));
        }
        for bucket in &mut buckets {
            bucket.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.commit_ts.cmp(&b.1.commit_ts)));
        }

        let mut directory = Vec::with_capacity(bucket_count);
        let mut pages = Vec::new();

        for bucket in buckets {
            let start_page = pages.len() as u32;
            let mut page = PageBuilder::new(self.options.page_size);
            for (key, row_id) in bucket {
                if !page.try_push(&key, row_id)? {
                    pages.push(page.finish());
                    page = PageBuilder::new(self.options.page_size);
                    let pushed = page.try_push(&key, row_id)?;
                    debug_assert!(pushed);
                }
            }
            if !page.is_empty() {
                pages.push(page.finish());
            }
            directory.push(BucketDirectoryEntry {
                start_page,
                page_count: pages.len() as u32 - start_page,
            });
        }

        let mut file_data =
            Vec::with_capacity(FILE_HEADER_LEN + directory.len() * BUCKET_DIRECTORY_ENTRY_LEN);
        file_data.extend_from_slice(&IMMUTABLE_INDEX_MAGIC);
        file_data.extend_from_slice(&IMMUTABLE_INDEX_FORMAT_VERSION.to_le_bytes());
        file_data.extend_from_slice(&(self.options.page_size as u32).to_le_bytes());
        file_data.extend_from_slice(&(bucket_count as u32).to_le_bytes());
        file_data.extend_from_slice(&(pages.len() as u32).to_le_bytes());
        file_data.extend_from_slice(&(DEFAULT_BLOOM_WORDS as u32).to_le_bytes());
        file_data.extend_from_slice(&0u32.to_le_bytes());
        file_data.extend_from_slice(&0u32.to_le_bytes());

        for entry in &directory {
            file_data.extend_from_slice(&entry.start_page.to_le_bytes());
            file_data.extend_from_slice(&entry.page_count.to_le_bytes());
        }

        for page in pages {
            file_data.extend_from_slice(&page);
        }

        let path = path.as_ref();
        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, &file_data).map_err(|e| {
            paro_error::io_error(format!("write immutable index {:?}: {}", tmp_path, e))
        })?;
        fs::rename(&tmp_path, path).map_err(|e| {
            paro_error::io_error(format!(
                "rename immutable index {:?} -> {:?}: {}",
                tmp_path, path, e
            ))
        })?;

        Ok(ImmutableIndexStats {
            bucket_count,
            page_count: (file_data.len()
                - FILE_HEADER_LEN
                - directory.len() * BUCKET_DIRECTORY_ENTRY_LEN)
                / self.options.page_size,
            entry_count: entries.len(),
        })
    }
}

#[derive(Debug)]
pub struct ImmutableIndexReader {
    path: PathBuf,
    bytes: Arc<[u8]>,
    page_size: usize,
    bucket_directory: Vec<BucketDirectoryEntry>,
    pages_offset: usize,
}

impl ImmutableIndexReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let bytes: Arc<[u8]> = fs::read(&path)
            .map(Arc::<[u8]>::from)
            .map_err(|e| paro_error::io_error(format!("read immutable index {:?}: {}", path, e)))?;
        Self::from_bytes(path, bytes)
    }

    pub fn open_cached(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let path = path.as_ref().to_path_buf();
        if let Some(reader) = IMMUTABLE_INDEX_CACHE
            .lock()
            .map_err(|_| paro_error::internal("immutable index cache lock poisoned"))?
            .get(&path)
            .and_then(Weak::upgrade)
        {
            return Ok(reader);
        }

        let reader = Arc::new(Self::open(&path)?);
        IMMUTABLE_INDEX_CACHE
            .lock()
            .map_err(|_| paro_error::internal("immutable index cache lock poisoned"))?
            .insert(path, Arc::downgrade(&reader));
        Ok(reader)
    }

    fn from_bytes(path: PathBuf, bytes: Arc<[u8]>) -> Result<Self> {
        if bytes.len() < FILE_HEADER_LEN {
            return Err(paro_error::data_corrupted(format!(
                "immutable index {:?} too small",
                path
            )));
        }
        if bytes[0..4] != IMMUTABLE_INDEX_MAGIC {
            return Err(paro_error::data_corrupted(format!(
                "immutable index {:?} magic mismatch",
                path
            )));
        }

        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if version != IMMUTABLE_INDEX_FORMAT_VERSION {
            return Err(paro_error::data_corrupted(format!(
                "unsupported immutable index version {} in {:?}",
                version, path
            )));
        }

        let page_size = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let bucket_count = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        let page_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let bloom_words = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
        if page_size <= PAGE_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "immutable index page size too small",
            ));
        }
        if bloom_words != DEFAULT_BLOOM_WORDS {
            return Err(paro_error::data_corrupted(format!(
                "unsupported immutable index bloom words {}",
                bloom_words
            )));
        }

        let directory_len = bucket_count * BUCKET_DIRECTORY_ENTRY_LEN;
        let pages_offset = FILE_HEADER_LEN + directory_len;
        let expected_len = pages_offset + page_count * page_size;
        if bytes.len() != expected_len {
            return Err(paro_error::data_corrupted(format!(
                "immutable index {:?} length mismatch: expected {}, got {}",
                path,
                expected_len,
                bytes.len()
            )));
        }

        let mut bucket_directory = Vec::with_capacity(bucket_count);
        let directory = &bytes[FILE_HEADER_LEN..pages_offset];
        for chunk in directory.chunks_exact(BUCKET_DIRECTORY_ENTRY_LEN) {
            bucket_directory.push(BucketDirectoryEntry {
                start_page: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
                page_count: u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
            });
        }

        Ok(Self {
            path,
            bytes,
            page_size,
            bucket_directory,
            pages_offset,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<RowID>> {
        Ok(self
            .get_version_at(key, u64::MAX)?
            .and_then(PrimaryIndexVersion::visible_row_id))
    }

    pub fn get_version_at(&self, key: &[u8], read_ts: u64) -> Result<Option<PrimaryIndexVersion>> {
        if self.bucket_directory.is_empty() {
            return Ok(None);
        }

        let bucket = bucket_index_for_key(key, self.bucket_directory.len());
        let entry = self.bucket_directory[bucket];
        if entry.page_count == 0 {
            return Ok(None);
        }

        let mut best = None;
        for page_idx in entry.start_page..entry.start_page + entry.page_count {
            let page = self.page(page_idx as usize)?;
            if !page.bloom.may_contain(key) {
                continue;
            }
            if let Some(version) = page.get_version_at(key, read_ts)? {
                if best
                    .map(|current: PrimaryIndexVersion| version.commit_ts > current.commit_ts)
                    .unwrap_or(true)
                {
                    best = Some(version);
                }
            }
        }

        Ok(best)
    }

    pub fn first_write_in_range(
        &self,
        key: &[u8],
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        if self.bucket_directory.is_empty() || read_ts >= commit_ts {
            return Ok(None);
        }

        let bucket = bucket_index_for_key(key, self.bucket_directory.len());
        let entry = self.bucket_directory[bucket];
        if entry.page_count == 0 {
            return Ok(None);
        }

        let mut best = None;
        for page_idx in entry.start_page..entry.start_page + entry.page_count {
            let page = self.page(page_idx as usize)?;
            if !page.bloom.may_contain(key) {
                continue;
            }
            if let Some(conflict) = page.first_write_in_range(key, read_ts, commit_ts)? {
                select_earlier_conflict(&mut best, conflict);
            }
        }

        Ok(best)
    }

    pub fn first_key_range_write_in_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        if self.bucket_directory.is_empty() || read_ts >= commit_ts {
            return Ok(None);
        }

        let mut best = None;
        for bucket in &self.bucket_directory {
            for page_idx in bucket.start_page..bucket.start_page + bucket.page_count {
                let page = self.page(page_idx as usize)?;
                if let Some(conflict) =
                    page.first_key_range_write_in_range(lower, upper, read_ts, commit_ts)?
                {
                    select_earlier_conflict(&mut best, conflict);
                }
            }
        }
        Ok(best)
    }

    pub fn entries(&self) -> Result<Vec<(Vec<u8>, PrimaryIndexVersion)>> {
        let mut out = Vec::new();
        for bucket in &self.bucket_directory {
            for page_idx in bucket.start_page..bucket.start_page + bucket.page_count {
                let page = self.page(page_idx as usize)?;
                page.extend_entries(&mut out)?;
            }
        }
        Ok(out)
    }

    pub fn page_count(&self) -> usize {
        (self.bytes.len() - self.pages_offset) / self.page_size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn page(&self, index: usize) -> Result<ImmutableIndexPage<'_>> {
        let start = self.pages_offset + index * self.page_size;
        let end = start + self.page_size;
        let page_bytes = self.bytes.get(start..end).ok_or_else(|| {
            paro_error::data_corrupted(format!(
                "immutable index page {} out of bounds for {:?}",
                index, self.path
            ))
        })?;
        ImmutableIndexPage::parse(page_bytes)
    }
}

struct PageBuilder {
    page_size: usize,
    records: Vec<(Vec<u8>, PrimaryIndexVersion)>,
    body_len: usize,
    bloom: PageBloom,
}

impl PageBuilder {
    fn new(page_size: usize) -> Self {
        Self {
            page_size,
            records: Vec::new(),
            body_len: 0,
            bloom: PageBloom::default(),
        }
    }

    fn try_push(&mut self, key: &[u8], version: PrimaryIndexVersion) -> Result<bool> {
        let record_len = 4 + key.len() + 16;
        let max_body_len = self.page_size - PAGE_HEADER_LEN;
        if record_len > max_body_len {
            return Err(paro_error::invalid_input(format!(
                "immutable index key of {} bytes does not fit into page",
                key.len()
            )));
        }
        if self.body_len + record_len > max_body_len && !self.records.is_empty() {
            return Ok(false);
        }
        self.body_len += record_len;
        self.bloom.add(key);
        self.records.push((key.to_vec(), version));
        Ok(true)
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn finish(self) -> Vec<u8> {
        let mut page = Vec::with_capacity(self.page_size);
        page.extend_from_slice(&(self.records.len() as u16).to_le_bytes());
        page.extend_from_slice(&PAGE_HEADER_RESERVED.to_le_bytes());
        page.extend_from_slice(&(self.body_len as u32).to_le_bytes());
        for word in self.bloom.words {
            page.extend_from_slice(&word.to_le_bytes());
        }
        for (key, version) in self.records {
            page.extend_from_slice(&(key.len() as u32).to_le_bytes());
            page.extend_from_slice(&key);
            page.extend_from_slice(&u64::from(version.row_id).to_le_bytes());
            page.extend_from_slice(&version.commit_ts.to_le_bytes());
        }
        page.resize(self.page_size, 0);
        page
    }
}

struct ImmutableIndexPage<'a> {
    entry_count: usize,
    body: &'a [u8],
    bloom: PageBloom,
}

impl<'a> ImmutableIndexPage<'a> {
    fn parse(page: &'a [u8]) -> Result<Self> {
        if page.len() < PAGE_HEADER_LEN {
            return Err(paro_error::data_corrupted(
                "immutable index page shorter than header",
            ));
        }

        let entry_count = u16::from_le_bytes(page[0..2].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(page[4..8].try_into().unwrap()) as usize;
        if PAGE_HEADER_LEN + data_len > page.len() {
            return Err(paro_error::data_corrupted(
                "immutable index page body out of bounds",
            ));
        }

        let mut words = [0u64; DEFAULT_BLOOM_WORDS];
        let mut offset = 8usize;
        for word in &mut words {
            *word = u64::from_le_bytes(page[offset..offset + 8].try_into().unwrap());
            offset += 8;
        }

        Ok(Self {
            entry_count,
            body: &page[PAGE_HEADER_LEN..PAGE_HEADER_LEN + data_len],
            bloom: PageBloom { words },
        })
    }

    fn get_version_at(&self, key: &[u8], read_ts: u64) -> Result<Option<PrimaryIndexVersion>> {
        let mut offset = 0usize;
        let mut best = None;
        for _ in 0..self.entry_count {
            let (current_key, version, next_offset) = read_record(self.body, offset)?;
            if current_key == key {
                if version.commit_ts <= read_ts
                    && best
                        .map(|current: PrimaryIndexVersion| version.commit_ts > current.commit_ts)
                        .unwrap_or(true)
                {
                    best = Some(version);
                }
            }
            if current_key > key {
                return Ok(best);
            }
            offset = next_offset;
        }
        Ok(best)
    }

    fn first_write_in_range(
        &self,
        key: &[u8],
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        let mut offset = 0usize;
        let mut best = None;
        for _ in 0..self.entry_count {
            let (current_key, version, next_offset) = read_record(self.body, offset)?;
            if current_key == key && version_in_window(version, read_ts, commit_ts) {
                select_earlier_conflict(
                    &mut best,
                    PrimaryKeyWriteConflict {
                        key: current_key.to_vec(),
                        version,
                    },
                );
            }
            if current_key > key {
                return Ok(best);
            }
            offset = next_offset;
        }
        Ok(best)
    }

    fn first_key_range_write_in_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        read_ts: u64,
        commit_ts: u64,
    ) -> Result<Option<PrimaryKeyWriteConflict>> {
        let mut offset = 0usize;
        let mut best = None;
        for _ in 0..self.entry_count {
            let (key, version, next_offset) = read_record(self.body, offset)?;
            if upper.map_or(false, |upper| key > upper) {
                return Ok(best);
            }
            if key_in_bounds(key, lower, upper) && version_in_window(version, read_ts, commit_ts) {
                select_earlier_conflict(
                    &mut best,
                    PrimaryKeyWriteConflict {
                        key: key.to_vec(),
                        version,
                    },
                );
            }
            offset = next_offset;
        }
        Ok(best)
    }

    fn extend_entries(&self, out: &mut Vec<(Vec<u8>, PrimaryIndexVersion)>) -> Result<()> {
        let mut offset = 0usize;
        for _ in 0..self.entry_count {
            let (key, row_id, next_offset) = read_record(self.body, offset)?;
            out.push((key.to_vec(), row_id));
            offset = next_offset;
        }
        Ok(())
    }
}

fn version_in_window(version: PrimaryIndexVersion, read_ts: u64, commit_ts: u64) -> bool {
    version.commit_ts > read_ts && version.commit_ts <= commit_ts
}

fn key_in_bounds(key: &[u8], lower: Option<&[u8]>, upper: Option<&[u8]>) -> bool {
    lower.map_or(true, |lower| key >= lower) && upper.map_or(true, |upper| key <= upper)
}

fn select_earlier_conflict(
    slot: &mut Option<PrimaryKeyWriteConflict>,
    candidate: PrimaryKeyWriteConflict,
) {
    let should_replace = slot
        .as_ref()
        .map(|current| {
            candidate.version.commit_ts < current.version.commit_ts
                || (candidate.version.commit_ts == current.version.commit_ts
                    && candidate.key < current.key)
        })
        .unwrap_or(true);
    if should_replace {
        *slot = Some(candidate);
    }
}

#[derive(Debug, Clone, Copy)]
struct PageBloom {
    words: [u64; DEFAULT_BLOOM_WORDS],
}

impl Default for PageBloom {
    fn default() -> Self {
        Self {
            words: [0u64; DEFAULT_BLOOM_WORDS],
        }
    }
}

impl PageBloom {
    fn add(&mut self, key: &[u8]) {
        for bit in bloom_bits_for_key(key) {
            let word_idx = bit / 64;
            let bit_idx = bit % 64;
            self.words[word_idx] |= 1u64 << bit_idx;
        }
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        bloom_bits_for_key(key).into_iter().all(|bit| {
            let word_idx = bit / 64;
            let bit_idx = bit % 64;
            (self.words[word_idx] & (1u64 << bit_idx)) != 0
        })
    }
}

fn normalize_entries(
    entries: &[(Vec<u8>, PrimaryIndexVersion)],
) -> Vec<(Vec<u8>, PrimaryIndexVersion)> {
    let mut normalized = BTreeMap::new();
    for (key, version) in entries {
        normalized.insert((key.clone(), version.commit_ts), *version);
    }
    normalized
        .into_iter()
        .map(|((key, _), version)| (key, version))
        .collect()
}

fn choose_bucket_count(entry_count: usize, target_bucket_entries: usize) -> usize {
    if entry_count == 0 {
        return 1;
    }
    let target = entry_count.div_ceil(target_bucket_entries.max(1));
    target.max(1).next_power_of_two()
}

fn bucket_index_for_key(key: &[u8], bucket_count: usize) -> usize {
    (seahash::hash(key) as usize) & (bucket_count - 1)
}

fn bloom_bits_for_key(key: &[u8]) -> [usize; BLOOM_HASH_ROUNDS] {
    let hash = seahash::hash(key);
    let mut out = [0usize; BLOOM_HASH_ROUNDS];
    for (idx, slot) in out.iter_mut().enumerate() {
        let rotated = hash.rotate_left((idx as u32) * 13);
        *slot = ((rotated ^ (rotated >> 17)) as usize) % (DEFAULT_BLOOM_WORDS * 64);
    }
    out
}

fn read_record(body: &[u8], offset: usize) -> Result<(&[u8], PrimaryIndexVersion, usize)> {
    if offset + 4 > body.len() {
        return Err(paro_error::data_corrupted(
            "immutable index page truncated before key length",
        ));
    }
    let key_len = u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()) as usize;
    let key_start = offset + 4;
    let value_start = key_start + key_len;
    let commit_ts_start = value_start + 8;
    let next_offset = commit_ts_start + 8;
    if next_offset > body.len() {
        return Err(paro_error::data_corrupted(
            "immutable index page truncated before value",
        ));
    }
    let key = &body[key_start..value_start];
    let row_id = RowID::from_raw(u64::from_le_bytes(
        body[value_start..commit_ts_start].try_into().unwrap(),
    ));
    let commit_ts = u64::from_le_bytes(body[commit_ts_start..next_offset].try_into().unwrap());
    Ok((
        key,
        PrimaryIndexVersion::live(row_id, commit_ts),
        next_offset,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn v(row_id: RowID, commit_ts: u64) -> PrimaryIndexVersion {
        PrimaryIndexVersion::live(row_id, commit_ts)
    }

    fn tombstone(commit_ts: u64) -> PrimaryIndexVersion {
        PrimaryIndexVersion::tombstone(commit_ts)
    }

    fn find_bloom_negative(page: &ImmutableIndexPage<'_>) -> Vec<u8> {
        for seed in 0..10_000u32 {
            let candidate = format!("missing-{seed:04}").into_bytes();
            if !page.bloom.may_contain(&candidate) {
                return candidate;
            }
        }
        panic!("failed to find bloom-negative candidate");
    }

    #[test]
    fn immutable_index_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("index.idx");
        let writer = ImmutableIndexWriter::default();
        writer
            .write_entries(
                &path,
                &[
                    (b"k1".to_vec(), v(RowID::new(1, 10), 1)),
                    (b"k2".to_vec(), v(RowID::new(2, 20), 1)),
                    (b"k3".to_vec(), v(RowID::new(3, 30), 1)),
                ],
            )
            .unwrap();

        let reader = ImmutableIndexReader::open(&path).unwrap();
        assert_eq!(reader.get(b"k1").unwrap(), Some(RowID::new(1, 10)));
        assert_eq!(reader.get(b"k3").unwrap(), Some(RowID::new(3, 30)));
        assert_eq!(reader.get(b"missing").unwrap(), None);
    }

    #[test]
    fn immutable_index_supports_multi_page_bucket() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("index.idx");
        let writer = ImmutableIndexWriter::new(ImmutableIndexBuildOptions {
            page_size: 256,
            target_bucket_entries: usize::MAX,
        });
        let entries: Vec<_> = (0..128u32)
            .map(|i| {
                (
                    format!("key-{i:04}-payload").into_bytes(),
                    v(RowID::new(7, i), 1),
                )
            })
            .collect();
        let stats = writer.write_entries(&path, &entries).unwrap();

        assert!(stats.page_count > 1);
        let reader = ImmutableIndexReader::open(&path).unwrap();
        assert_eq!(
            reader.get(b"key-0042-payload").unwrap(),
            Some(RowID::new(7, 42))
        );
    }

    #[test]
    fn immutable_index_preserves_tombstones() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("index.idx");
        ImmutableIndexWriter::default()
            .write_entries(
                &path,
                &[
                    (b"gone".to_vec(), tombstone(2)),
                    (b"live".to_vec(), v(RowID::new(9, 1), 1)),
                ],
            )
            .unwrap();

        let reader = ImmutableIndexReader::open(&path).unwrap();
        assert_eq!(reader.get(b"gone").unwrap(), None);
        assert_eq!(
            reader.get_version_at(b"gone", u64::MAX).unwrap(),
            Some(tombstone(2))
        );
        assert_eq!(reader.get(b"live").unwrap(), Some(RowID::new(9, 1)));
    }

    #[test]
    fn immutable_index_normalizes_duplicates_and_keeps_sorted_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("index.idx");
        let stats = ImmutableIndexWriter::default()
            .write_entries(
                &path,
                &[
                    (b"k2".to_vec(), v(RowID::new(1, 2), 1)),
                    (b"k1".to_vec(), v(RowID::new(1, 1), 1)),
                    (b"k2".to_vec(), v(RowID::new(9, 9), 1)),
                ],
            )
            .unwrap();

        assert_eq!(stats.entry_count, 2);
        let reader = ImmutableIndexReader::open(&path).unwrap();
        assert_eq!(
            reader.entries().unwrap(),
            vec![
                (b"k1".to_vec(), v(RowID::new(1, 1), 1)),
                (b"k2".to_vec(), v(RowID::new(9, 9), 1)),
            ]
        );
    }

    #[test]
    fn immutable_index_lookup_filters_by_read_ts() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("index.idx");
        ImmutableIndexWriter::default()
            .write_entries(
                &path,
                &[
                    (b"k".to_vec(), v(RowID::new(1, 1), 10)),
                    (b"k".to_vec(), v(RowID::new(2, 2), 20)),
                    (b"k".to_vec(), tombstone(30)),
                ],
            )
            .unwrap();

        let reader = ImmutableIndexReader::open(&path).unwrap();
        assert_eq!(reader.get_version_at(b"k", 9).unwrap(), None);
        assert_eq!(reader.get(b"k").unwrap(), None);
        assert_eq!(
            reader.get_version_at(b"k", 10).unwrap(),
            Some(v(RowID::new(1, 1), 10))
        );
        assert_eq!(
            reader.get_version_at(b"k", 29).unwrap(),
            Some(v(RowID::new(2, 2), 20))
        );
        assert_eq!(
            reader.get_version_at(b"k", 30).unwrap(),
            Some(tombstone(30))
        );
    }

    #[test]
    fn immutable_index_write_conflict_window_finds_point_and_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("index.idx");
        ImmutableIndexWriter::default()
            .write_entries(
                &path,
                &[
                    (b"a".to_vec(), v(RowID::new(1, 1), 10)),
                    (b"b".to_vec(), v(RowID::new(1, 2), 20)),
                    (b"b".to_vec(), tombstone(30)),
                    (b"z".to_vec(), v(RowID::new(1, 9), 40)),
                ],
            )
            .unwrap();

        let reader = ImmutableIndexReader::open(&path).unwrap();
        assert!(reader.first_write_in_range(b"a", 10, 30).unwrap().is_none());
        assert_eq!(
            reader
                .first_write_in_range(b"b", 19, 30)
                .unwrap()
                .unwrap()
                .commit_ts(),
            20
        );
        let conflict = reader
            .first_key_range_write_in_range(Some(b"b"), Some(b"y"), 20, 30)
            .unwrap()
            .unwrap();
        assert_eq!(conflict.key, b"b".to_vec());
        assert!(conflict.is_tombstone());
    }

    #[test]
    fn immutable_index_page_bloom_tracks_inserted_keys() {
        let mut builder = PageBuilder::new(256);
        builder.try_push(b"alpha", v(RowID::new(1, 1), 1)).unwrap();
        builder.try_push(b"bravo", v(RowID::new(1, 2), 1)).unwrap();
        let page_bytes = builder.finish();
        let page = ImmutableIndexPage::parse(&page_bytes).unwrap();

        assert!(page.bloom.may_contain(b"alpha"));
        assert!(page.bloom.may_contain(b"bravo"));

        let missing = find_bloom_negative(&page);
        assert!(!page.bloom.may_contain(&missing));
        assert_eq!(page.get_version_at(&missing, u64::MAX).unwrap(), None);
    }

    #[test]
    fn immutable_index_cache_reuses_reader() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("index.idx");
        ImmutableIndexWriter::default()
            .write_entries(&path, &[(b"k".to_vec(), v(RowID::new(1, 1), 1))])
            .unwrap();

        let first = ImmutableIndexReader::open_cached(&path).unwrap();
        let second = ImmutableIndexReader::open_cached(&path).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }
}
