//! # WAL Checksum
//!
//! Checksum helpers for WAL entry integrity.
//!
//! Uses a combination of multiplicative hashing and MurmurHash-style mixing.
const HASH_MULTIPLIER: u64 = 0xbf58476d1ce4e5b9;

/// MurmurHash-style constants.
const MURMUR_M: u64 = 0xc6a4a7935bd1e995;
const MURMUR_SEED: u64 = 0xe17a1465;
const MURMUR_R: u32 = 47;

/// Compute checksum for a single u64 value.
#[inline]
fn checksum_u64(x: u64) -> u64 {
    x.wrapping_mul(HASH_MULTIPLIER)
}

/// Compute MurmurHash-style checksum for remaining bytes (0-7 bytes).
///
/// Based on robin-hood-hashing implementation.
fn checksum_remainder(data: &[u8]) -> u64 {
    let len = data.len();
    let mut h = MURMUR_SEED ^ ((len as u64).wrapping_mul(MURMUR_M));

    // Process 8-byte chunks
    let n_blocks = len / 8;
    for i in 0..n_blocks {
        let offset = i * 8;
        let k = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());

        let k = k.wrapping_mul(MURMUR_M);
        let k = k ^ (k >> MURMUR_R);
        let k = k.wrapping_mul(MURMUR_M);

        h ^= k;
        h = h.wrapping_mul(MURMUR_M);
    }

    // Process remaining bytes
    let remainder = &data[n_blocks * 8..];
    match remainder.len() {
        7 => {
            h ^= (remainder[6] as u64) << 48;
            h ^= (remainder[5] as u64) << 40;
            h ^= (remainder[4] as u64) << 32;
            h ^= (remainder[3] as u64) << 24;
            h ^= (remainder[2] as u64) << 16;
            h ^= (remainder[1] as u64) << 8;
            h ^= remainder[0] as u64;
            h = h.wrapping_mul(MURMUR_M);
        }
        6 => {
            h ^= (remainder[5] as u64) << 40;
            h ^= (remainder[4] as u64) << 32;
            h ^= (remainder[3] as u64) << 24;
            h ^= (remainder[2] as u64) << 16;
            h ^= (remainder[1] as u64) << 8;
            h ^= remainder[0] as u64;
            h = h.wrapping_mul(MURMUR_M);
        }
        5 => {
            h ^= (remainder[4] as u64) << 32;
            h ^= (remainder[3] as u64) << 24;
            h ^= (remainder[2] as u64) << 16;
            h ^= (remainder[1] as u64) << 8;
            h ^= remainder[0] as u64;
            h = h.wrapping_mul(MURMUR_M);
        }
        4 => {
            h ^= (remainder[3] as u64) << 24;
            h ^= (remainder[2] as u64) << 16;
            h ^= (remainder[1] as u64) << 8;
            h ^= remainder[0] as u64;
            h = h.wrapping_mul(MURMUR_M);
        }
        3 => {
            h ^= (remainder[2] as u64) << 16;
            h ^= (remainder[1] as u64) << 8;
            h ^= remainder[0] as u64;
            h = h.wrapping_mul(MURMUR_M);
        }
        2 => {
            h ^= (remainder[1] as u64) << 8;
            h ^= remainder[0] as u64;
            h = h.wrapping_mul(MURMUR_M);
        }
        1 => {
            h ^= remainder[0] as u64;
            h = h.wrapping_mul(MURMUR_M);
        }
        _ => {}
    }

    h ^= h >> MURMUR_R;
    h = h.wrapping_mul(MURMUR_M);
    h ^= h >> MURMUR_R;
    h
}
///
/// The algorithm:
/// 1. Process data in 8-byte chunks using multiplicative hashing
/// 2. Handle remaining bytes (0-7) using MurmurHash-style mixing
/// 3. XOR all results together
///
/// # Arguments
/// * `buffer` - The data to checksum
///
/// # Returns
/// A 64-bit checksum value
pub fn compute_wal_checksum(buffer: &[u8]) -> u64 {
    let mut result: u64 = 5381; // Initial seed for the checksum routine.

    // Process 8-byte chunks
    let n_chunks = buffer.len() / 8;
    for i in 0..n_chunks {
        let offset = i * 8;
        let value = u64::from_le_bytes(buffer[offset..offset + 8].try_into().unwrap());
        result ^= checksum_u64(value);
    }

    // Handle remaining bytes
    let remainder_start = n_chunks * 8;
    if remainder_start < buffer.len() {
        result ^= checksum_remainder(&buffer[remainder_start..]);
    }

    result
}

/// Verify a checksum against expected value.
#[inline]
// Used by unit tests here; readers may call this when validating WAL payloads.
#[allow(dead_code)]
pub fn verify_checksum(buffer: &[u8], expected: u64) -> bool {
    compute_wal_checksum(buffer) == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_buffer() {
        let checksum = compute_wal_checksum(&[]);
        // Empty buffer should return the seed value XOR'd with remainder hash
        assert_ne!(checksum, 0);
    }

    #[test]
    fn test_single_byte() {
        let checksum1 = compute_wal_checksum(&[0x42]);
        let checksum2 = compute_wal_checksum(&[0x43]);
        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_eight_bytes() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let checksum = compute_wal_checksum(&data);
        assert_ne!(checksum, 0);
    }

    #[test]
    fn test_deterministic() {
        let data = b"Hello, WAL!";
        let checksum1 = compute_wal_checksum(data);
        let checksum2 = compute_wal_checksum(data);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_different_data() {
        let data1 = b"Hello, WAL!";
        let data2 = b"Hello, WAL?";
        let checksum1 = compute_wal_checksum(data1);
        let checksum2 = compute_wal_checksum(data2);
        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_verify_checksum() {
        let data = b"Test data for checksum verification";
        let checksum = compute_wal_checksum(data);
        assert!(verify_checksum(data, checksum));
        assert!(!verify_checksum(data, checksum + 1));
    }

    #[test]
    fn test_various_lengths() {
        // Test all remainder lengths (0-7)
        for len in 0..16 {
            let data: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let checksum = compute_wal_checksum(&data);
            assert!(verify_checksum(&data, checksum));
        }
    }
}
