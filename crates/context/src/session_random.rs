// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Session-owned random number generation.
//!
//! SQL volatile functions must draw from state whose lifetime is the session,
//! not from the wall clock at each evaluation. Keeping the generator here lets
//! immutable statement snapshots share the same sequence without making the
//! execution or function crates depend on the session crate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug)]
pub struct SessionRandom {
    state: Mutex<u64>,
}

impl SessionRandom {
    pub fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let time_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as u64)
            .unwrap_or(0x9e37_79b9_7f4a_7c15);
        Self::with_seed(time_seed ^ SEQUENCE.fetch_add(1, Ordering::Relaxed))
    }

    pub fn with_seed(seed: u64) -> Self {
        Self {
            state: Mutex::new(seed),
        }
    }

    /// Reset the deterministic session sequence from PostgreSQL's floating
    /// seed domain. Zero is valid because SplitMix64 has no absorbing state.
    pub fn set_seed(&self, seed: f64) {
        let normalized = seed.clamp(-1.0, 1.0);
        let bits = normalized.to_bits() ^ 0x9e37_79b9_7f4a_7c15;
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = bits;
    }

    pub fn set_seed_raw(&self, seed: u64) {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = seed;
    }

    pub fn next_u64(&self) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = *state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    pub fn next_f64(&self) -> f64 {
        const SCALE: f64 = 1.0 / ((1_u64 << 53) as f64);
        ((self.next_u64() >> 11) as f64) * SCALE
    }

    pub fn fill_bytes(&self, data: &mut [u8]) {
        for chunk in data.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

impl Default for SessionRandom {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_sessions_produce_the_same_sequence() {
        let first = SessionRandom::with_seed(42);
        let second = SessionRandom::with_seed(42);
        for _ in 0..32 {
            assert_eq!(first.next_u64(), second.next_u64());
        }
    }

    #[test]
    fn zero_seed_is_not_an_absorbing_state() {
        let random = SessionRandom::with_seed(0);
        assert_ne!(random.next_u64(), 0);
        assert_ne!(random.next_u64(), 0);
    }

    #[test]
    fn floating_values_use_the_half_open_unit_interval() {
        let random = SessionRandom::with_seed(7);
        for _ in 0..1_000 {
            assert!((0.0..1.0).contains(&random.next_f64()));
        }
    }
}
