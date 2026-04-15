//! # Bloom Filter Index Implementation
//!
//! Per-page bloom filters for accelerating equality queries.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use paro_common::error::{self as paro_error, Result};
use std::hash::{Hash, Hasher};

/// Hash strategy for bloom filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HashStrategy {
    /// MurmurHash3 x64 64-bit
    MurmurHash3X64_64 = 0,
}

impl HashStrategy {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(HashStrategy::MurmurHash3X64_64),
            _ => Err(paro_error::not_supported(format!(
                "Unknown hash strategy: {}",
                v
            ))),
        }
    }
}

/// Bloom filter algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BloomFilterAlgorithm {
    /// Block bloom filter (cache-friendly)
    Block = 0,
    /// Classic bloom filter
    Classic = 1,
}

impl BloomFilterAlgorithm {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(BloomFilterAlgorithm::Block),
            1 => Ok(BloomFilterAlgorithm::Classic),
            _ => Err(paro_error::not_supported(format!(
                "Unknown bloom filter algorithm: {}",
                v
            ))),
        }
    }
}

/// Bloom filter options.
#[derive(Debug, Clone)]
pub struct BloomFilterOptions {
    /// Hash strategy
    pub hash_strategy: HashStrategy,
    /// Bloom filter algorithm
    pub algorithm: BloomFilterAlgorithm,
    /// False positive probability (default: 0.05)
    pub fpp: f64,
    /// Expected number of distinct values per page
    pub expected_entries: usize,
}

impl Default for BloomFilterOptions {
    fn default() -> Self {
        BloomFilterOptions {
            hash_strategy: HashStrategy::MurmurHash3X64_64,
            algorithm: BloomFilterAlgorithm::Block,
            fpp: 0.05,
            expected_entries: 1024,
        }
    }
}

impl BloomFilterOptions {
    /// Create new options with custom FPP.
    pub fn with_fpp(mut self, fpp: f64) -> Self {
        self.fpp = fpp;
        self
    }

    /// Create new options with expected entries.
    pub fn with_expected_entries(mut self, entries: usize) -> Self {
        self.expected_entries = entries;
        self
    }

    /// Calculate optimal number of bits.
    fn optimal_num_bits(&self) -> usize {
        let n = self.expected_entries as f64;
        let p = self.fpp;
        // m = -n * ln(p) / (ln(2)^2)
        let m = -n * p.ln() / (2.0_f64.ln().powi(2));
        // Round up to multiple of 64 for alignment
        (m as usize).div_ceil(64) * 64
    }

    /// Calculate optimal number of hash functions.
    fn optimal_num_hashes(&self) -> usize {
        let m = self.optimal_num_bits() as f64;
        let n = self.expected_entries as f64;
        // k = (m/n) * ln(2)
        let k = (m / n) * 2.0_f64.ln();
        std::cmp::max(1, k.round() as usize)
    }
}

/// A simple bloom filter implementation.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array
    bits: Vec<u64>,
    /// Number of hash functions
    num_hashes: usize,
    /// Number of bits
    num_bits: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with the given options.
    pub fn new(opts: &BloomFilterOptions) -> Self {
        let num_bits = opts.optimal_num_bits();
        let num_hashes = opts.optimal_num_hashes();
        let num_words = num_bits.div_ceil(64);

        BloomFilter {
            bits: vec![0u64; num_words],
            num_hashes,
            num_bits,
        }
    }

    /// Create a bloom filter from raw bits.
    pub fn from_bits(bits: Vec<u64>, num_hashes: usize) -> Self {
        let num_bits = bits.len() * 64;
        BloomFilter {
            bits,
            num_hashes,
            num_bits,
        }
    }

    /// Add a value to the bloom filter.
    pub fn add(&mut self, value: &[u8]) {
        let (h1, h2) = self.hash(value);
        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;
            if word_idx < self.bits.len() {
                self.bits[word_idx] |= 1u64 << bit_offset;
            }
        }
    }

    /// Check if a value might be in the bloom filter.
    ///
    /// Returns true if the value might be present (possible false positive),
    /// false if the value is definitely not present.
    pub fn may_contain(&self, value: &[u8]) -> bool {
        let (h1, h2) = self.hash(value);
        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;
            if word_idx >= self.bits.len() || (self.bits[word_idx] & (1u64 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    /// Get the bit index for the i-th hash.
    fn get_bit_index(&self, h1: u64, h2: u64, i: usize) -> usize {
        // Double hashing: h(i) = h1 + i * h2
        let hash = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (hash % self.num_bits as u64) as usize
    }

    /// Compute two hash values using MurmurHash3-like algorithm.
    fn hash(&self, value: &[u8]) -> (u64, u64) {
        // Simple hash implementation using std::hash
        // In production, use a proper MurmurHash3 implementation
        let mut hasher1 = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher1);
        let h1 = hasher1.finish();

        let mut hasher2 = std::collections::hash_map::DefaultHasher::new();
        h1.hash(&mut hasher2);
        let h2 = hasher2.finish();

        (h1, h2)
    }

    /// Serialize the bloom filter.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + self.bits.len() * 8);
        buf.put_u32_le(self.num_hashes as u32);
        buf.put_u32_le(self.bits.len() as u32);
        for &word in &self.bits {
            buf.put_u64_le(word);
        }
        buf.freeze()
    }

    /// Deserialize a bloom filter.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(paro_error::data_corrupted("BloomFilter: data too small"));
        }

        let mut buf = data;
        let num_hashes = buf.get_u32_le() as usize;
        let num_words = buf.get_u32_le() as usize;

        if buf.remaining() < num_words * 8 {
            return Err(paro_error::data_corrupted("BloomFilter: truncated data"));
        }

        let mut bits = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            bits.push(buf.get_u64_le());
        }

        Ok(BloomFilter::from_bits(bits, num_hashes))
    }

    /// Get the number of bits.
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Get the number of hash functions.
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Get the raw bits.
    pub fn bits(&self) -> &[u64] {
        &self.bits
    }
}

/// Bloom filter index writer.
#[derive(Debug)]
pub struct BloomFilterIndexWriter {
    /// Options
    opts: BloomFilterOptions,
    /// Current page's bloom filter
    current_bf: BloomFilter,
    /// Serialized bloom filters for each page
    bloom_filters: Vec<Bytes>,
    /// Whether current bloom filter has data
    has_data: bool,
}

impl BloomFilterIndexWriter {
    /// Create a new bloom filter index writer.
    pub fn new(opts: BloomFilterOptions) -> Self {
        let current_bf = BloomFilter::new(&opts);
        BloomFilterIndexWriter {
            opts,
            current_bf,
            bloom_filters: Vec::new(),
            has_data: false,
        }
    }

    /// Add values to the current page's bloom filter.
    pub fn add_values(&mut self, values: &[&[u8]]) {
        for value in values {
            self.current_bf.add(value);
            self.has_data = true;
        }
    }

    /// Add a single value.
    pub fn add_value(&mut self, value: &[u8]) {
        self.current_bf.add(value);
        self.has_data = true;
    }

    /// Add null values (does not set bits, but ensures the page is recorded).
    pub fn add_nulls(&mut self, _count: u32) {
        // Nulls are not added to bloom filter, but we still need to
        // mark that the current page has data so we emit an empty filter.
        if _count > 0 {
            self.has_data = true;
        }
    }

    /// Flush the current page's bloom filter.
    pub fn flush(&mut self) {
        if self.has_data {
            self.bloom_filters.push(self.current_bf.to_bytes());
            self.current_bf = BloomFilter::new(&self.opts);
            self.has_data = false;
        }
    }

    /// Finish and serialize the index.
    ///
    /// Format:
    /// ```text
    /// hash_strategy(1) | algorithm(1) | num_filters(4)
    /// [filter_len(4) | filter_data] * num_filters
    /// ```
    pub fn finish(&mut self) -> Bytes {
        // Flush any remaining data
        self.flush();

        let mut buf = BytesMut::new();

        // Write header
        buf.put_u8(self.opts.hash_strategy as u8);
        buf.put_u8(self.opts.algorithm as u8);
        buf.put_u32_le(self.bloom_filters.len() as u32);

        // Write bloom filters
        for bf in &self.bloom_filters {
            buf.put_u32_le(bf.len() as u32);
            buf.extend_from_slice(bf);
        }

        buf.freeze()
    }

    /// Get the number of bloom filters written.
    pub fn num_filters(&self) -> usize {
        self.bloom_filters.len()
    }

    /// Get the total size in bytes.
    pub fn size(&self) -> usize {
        6 + self
            .bloom_filters
            .iter()
            .map(|bf| 4 + bf.len())
            .sum::<usize>()
    }
}

/// Bloom filter index reader.
#[derive(Debug)]
pub struct BloomFilterIndexReader {
    /// Hash strategy
    hash_strategy: HashStrategy,
    /// Algorithm
    algorithm: BloomFilterAlgorithm,
    /// Serialized bloom filters
    bloom_filters: Vec<Bytes>,
}

impl BloomFilterIndexReader {
    /// Create from serialized index data.
    pub fn from_bytes(data: &Bytes) -> Result<Self> {
        if data.len() < 6 {
            return Err(paro_error::data_corrupted(
                "BloomFilterIndexReader: data too small",
            ));
        }

        let mut buf = data.as_ref();

        let hash_strategy = HashStrategy::from_u8(buf.get_u8())?;
        let algorithm = BloomFilterAlgorithm::from_u8(buf.get_u8())?;
        let num_filters = buf.get_u32_le() as usize;

        let mut bloom_filters = Vec::with_capacity(num_filters);
        for _ in 0..num_filters {
            if buf.remaining() < 4 {
                return Err(paro_error::data_corrupted(
                    "BloomFilterIndexReader: truncated filter length",
                ));
            }
            let len = buf.get_u32_le() as usize;
            if buf.remaining() < len {
                return Err(paro_error::data_corrupted(
                    "BloomFilterIndexReader: truncated filter data",
                ));
            }
            bloom_filters.push(Bytes::copy_from_slice(&buf[..len]));
            buf.advance(len);
        }

        Ok(BloomFilterIndexReader {
            hash_strategy,
            algorithm,
            bloom_filters,
        })
    }

    /// Get the number of bloom filters.
    pub fn num_filters(&self) -> usize {
        self.bloom_filters.len()
    }

    /// Read bloom filter for a page.
    pub fn read_bloom_filter(&self, page_idx: usize) -> Result<BloomFilter> {
        let data = self.bloom_filters.get(page_idx).ok_or_else(|| {
            paro_error::out_of_range(format!(
                "BloomFilterIndexReader: page {} out of range",
                page_idx
            ))
        })?;
        BloomFilter::from_bytes(data)
    }

    /// Check if a page might contain a value.
    pub fn page_may_contain(&self, page_idx: usize, value: &[u8]) -> Result<bool> {
        let bf = self.read_bloom_filter(page_idx)?;
        Ok(bf.may_contain(value))
    }

    /// Get hash strategy.
    pub fn hash_strategy(&self) -> HashStrategy {
        self.hash_strategy
    }

    /// Get algorithm.
    pub fn algorithm(&self) -> BloomFilterAlgorithm {
        self.algorithm
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let opts = BloomFilterOptions::default();
        let mut bf = BloomFilter::new(&opts);

        bf.add(b"hello");
        bf.add(b"world");

        assert!(bf.may_contain(b"hello"));
        assert!(bf.may_contain(b"world"));
        // This might be a false positive, but unlikely
        assert!(!bf.may_contain(b"definitely_not_present_12345"));
    }

    #[test]
    fn test_bloom_filter_roundtrip() {
        let opts = BloomFilterOptions::default();
        let mut bf = BloomFilter::new(&opts);

        bf.add(b"test1");
        bf.add(b"test2");
        bf.add(b"test3");

        let data = bf.to_bytes();
        let bf2 = BloomFilter::from_bytes(&data).unwrap();

        assert!(bf2.may_contain(b"test1"));
        assert!(bf2.may_contain(b"test2"));
        assert!(bf2.may_contain(b"test3"));
    }

    #[test]
    fn test_bloom_filter_index_roundtrip() {
        let opts = BloomFilterOptions::default();
        let mut writer = BloomFilterIndexWriter::new(opts);

        // Page 0
        writer.add_value(b"apple");
        writer.add_value(b"banana");
        writer.flush();

        // Page 1
        writer.add_value(b"cherry");
        writer.add_value(b"date");
        writer.flush();

        let data = writer.finish();
        let reader = BloomFilterIndexReader::from_bytes(&data).unwrap();

        assert_eq!(reader.num_filters(), 2);

        // Check page 0
        assert!(reader.page_may_contain(0, b"apple").unwrap());
        assert!(reader.page_may_contain(0, b"banana").unwrap());

        // Check page 1
        assert!(reader.page_may_contain(1, b"cherry").unwrap());
        assert!(reader.page_may_contain(1, b"date").unwrap());
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let opts = BloomFilterOptions::default()
            .with_fpp(0.01)
            .with_expected_entries(1000);
        let mut bf = BloomFilter::new(&opts);

        // Add 1000 values
        for i in 0i32..1000 {
            bf.add(&i.to_le_bytes());
        }

        // Check false positive rate
        let mut false_positives = 0;
        let test_count = 10000i32;
        for i in 1000..(1000 + test_count) {
            if bf.may_contain(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }

        let fpp = false_positives as f64 / test_count as f64;
        // Allow some margin for statistical variation
        assert!(fpp < 0.05, "False positive rate {} is too high", fpp);
    }
}
