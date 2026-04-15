//! # HNSW Build Distance Cache
//!
//! A tiny direct-mapped cache used by heuristic link selection during graph build.

use std::cmp::{max, min};
use std::hash::{Hash, Hasher};

use seahash::SeaHasher;

use crate::index::hnsw::types::{PointOffset, ScoreType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PointPair {
    a: PointOffset,
    b: PointOffset,
}

impl PointPair {
    fn new(a: PointOffset, b: PointOffset) -> Self {
        Self {
            a: min(a, b),
            b: max(a, b),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CacheEntry {
    pair: PointPair,
    score: ScoreType,
}

/// Distance cache for `(point_a, point_b) -> score`.
#[derive(Debug)]
pub struct DistanceCache {
    entries: Vec<Option<CacheEntry>>,
}

impl DistanceCache {
    pub fn new(slots: usize) -> Self {
        let slots = slots.max(1);
        Self {
            entries: vec![None; slots],
        }
    }

    fn slot(&self, pair: PointPair) -> usize {
        let mut hasher = SeaHasher::new();
        pair.hash(&mut hasher);
        hasher.finish() as usize % self.entries.len()
    }

    pub fn get(&self, point_a: PointOffset, point_b: PointOffset) -> Option<ScoreType> {
        let pair = PointPair::new(point_a, point_b);
        let slot = self.slot(pair);
        self.entries[slot]
            .filter(|entry| entry.pair == pair)
            .map(|entry| entry.score)
    }

    pub fn put(&mut self, point_a: PointOffset, point_b: PointOffset, score: ScoreType) {
        let pair = PointPair::new(point_a, point_b);
        let slot = self.slot(pair);
        self.entries[slot] = Some(CacheEntry { pair, score });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_cache_roundtrip_is_symmetric() {
        let mut cache = DistanceCache::new(1024);
        cache.put(100, 7, 0.88);
        cache.put(11, 33, 0.42);

        assert_eq!(cache.get(7, 100), Some(0.88));
        assert_eq!(cache.get(100, 7), Some(0.88));
        assert_eq!(cache.get(11, 33), Some(0.42));
        assert_eq!(cache.get(33, 11), Some(0.42));
        assert_eq!(cache.get(1, 2), None);
    }

    #[test]
    fn distance_cache_single_slot_overwrites_on_collision() {
        let mut cache = DistanceCache::new(1);
        cache.put(1, 2, 0.8);
        cache.put(3, 4, 0.7);

        assert_eq!(cache.get(1, 2), None);
        assert_eq!(cache.get(2, 1), None);
        assert_eq!(cache.get(4, 3), Some(0.7));
    }
}
