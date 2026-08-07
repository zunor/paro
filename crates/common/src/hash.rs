// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Fast deterministic hashing primitives for in-memory execution structures.
//!
//! These hashes are deliberately not a persistence format. Hash joins,
//! aggregates, and runtime filters should share them so a physical value has
//! one execution-time hash implementation instead of paying for independent
//! standard-library hashers in each operator.

use std::hash::{BuildHasher, Hasher};

pub const HASH_SEED: u64 = 0xa076_1d64_78bd_642f;
pub const HASH_FIELD_SEED: u64 = 0xe703_7ed1_a0b4_28db;
pub const NULL_HASH: u64 = 0x9e37_79b9_7f4a_7c15;

/// Builder for hash-based execution structures whose keys are trusted query
/// values rather than adversarial protocol input.
///
/// Rust's default map hasher is deliberately collision resistant. Execution
/// operators already use the deterministic kernels in this module, so using
/// the same family for typed sets avoids paying SipHash for every numeric key.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecutionHashBuilder;

impl BuildHasher for ExecutionHashBuilder {
    type Hasher = ExecutionHasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        ExecutionHasher { state: HASH_SEED }
    }
}

/// Incremental adapter used by [`ExecutionHashBuilder`].
#[derive(Debug, Clone, Copy)]
pub struct ExecutionHasher {
    state: u64,
}

impl ExecutionHasher {
    #[inline]
    fn push(&mut self, hash: u64) {
        self.state = combine_hash(self.state, hash);
    }
}

impl Hasher for ExecutionHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.state
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.push(hash_bytes(bytes));
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.push(hash_u64(value as u64));
    }

    #[inline]
    fn write_u16(&mut self, value: u16) {
        self.push(hash_u64(value as u64));
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.push(hash_u64(value as u64));
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.push(hash_u64(value));
    }

    #[inline]
    fn write_u128(&mut self, value: u128) {
        self.push(hash_u128(value));
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.push(hash_u64(value as u64));
    }

    #[inline]
    fn write_i8(&mut self, value: i8) {
        self.push(hash_i64(value as i64));
    }

    #[inline]
    fn write_i16(&mut self, value: i16) {
        self.push(hash_i64(value as i64));
    }

    #[inline]
    fn write_i32(&mut self, value: i32) {
        self.push(hash_i64(value as i64));
    }

    #[inline]
    fn write_i64(&mut self, value: i64) {
        self.push(hash_i64(value));
    }

    #[inline]
    fn write_i128(&mut self, value: i128) {
        self.push(hash_u128(value as u128));
    }

    #[inline]
    fn write_isize(&mut self, value: isize) {
        self.push(hash_i64(value as i64));
    }
}

#[inline]
pub fn hash_i64(value: i64) -> u64 {
    hash_u64(value as u64)
}

#[inline]
pub fn hash_u128(value: u128) -> u64 {
    combine_hash(hash_u64(value as u64), hash_u64((value >> 64) as u64))
}

#[inline]
pub fn hash_u64(value: u64) -> u64 {
    let left = value.wrapping_mul(HASH_SEED) ^ HASH_SEED;
    let right = value.rotate_left(31).wrapping_mul(HASH_FIELD_SEED) ^ HASH_FIELD_SEED;
    mul_xor_mix(left, right)
}

pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = combine_hash(hash_u64(bytes.len() as u64), HASH_FIELD_SEED);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in chunks.by_ref() {
        // SAFETY: `chunks_exact(8)` guarantees eight readable bytes. The input
        // has no alignment contract, so the load is explicitly unaligned.
        let word = unsafe { std::ptr::read_unaligned(chunk.as_ptr() as *const u64) };
        hash = combine_hash(hash, hash_u64(u64::from_le(word)));
    }
    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let tail = remainder
            .iter()
            .enumerate()
            .fold(0_u64, |word, (idx, byte)| {
                word | (*byte as u64) << (idx * u8::BITS as usize)
            });
        hash = combine_hash(hash, hash_u64(tail));
    }
    hash
}

#[inline]
pub fn combine_hash(left: u64, right: u64) -> u64 {
    mul_xor_mix(
        left ^ HASH_FIELD_SEED,
        right.wrapping_add(0x9e37_79b9_7f4a_7c15),
    )
}

#[inline]
fn mul_xor_mix(left: u64, right: u64) -> u64 {
    let product = (left as u128).wrapping_mul(right as u128);
    ((product >> 64) as u64) ^ (product as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_hash_bytes(bytes: &[u8]) -> u64 {
        let mut hash = combine_hash(hash_u64(bytes.len() as u64), HASH_FIELD_SEED);
        let mut chunks = bytes.chunks_exact(8);
        for chunk in chunks.by_ref() {
            let mut word = [0_u8; 8];
            word.copy_from_slice(chunk);
            hash = combine_hash(hash, hash_u64(u64::from_le_bytes(word)));
        }
        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            let mut tail = [0_u8; 8];
            tail[..remainder.len()].copy_from_slice(remainder);
            hash = combine_hash(hash, hash_u64(u64::from_le_bytes(tail)));
        }
        hash
    }

    #[test]
    fn byte_hash_matches_reference_for_unaligned_slices() {
        let bytes = (0..80).map(|value| value as u8).collect::<Vec<_>>();
        for offset in 0..8 {
            for len in 0..=64 {
                let input = &bytes[offset..offset + len];
                assert_eq!(hash_bytes(input), reference_hash_bytes(input));
            }
        }
    }
}
