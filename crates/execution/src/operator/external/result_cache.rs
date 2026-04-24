// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::hash::{Hash, Hasher};

use paro_common::chunk::Chunk;
use paro_routine::RoutineCallIdentity;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryLocalResultCacheKey {
    pub routine_identities: Vec<RoutineCallIdentity>,
    pub null_pattern: String,
    pub abi_view_digest: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryLocalResultCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub admissions: u64,
    pub evictions: u64,
    pub rejected: u64,
    pub resident_bytes: u64,
}

#[derive(Debug, Clone)]
struct QueryLocalResultCacheEntry {
    result: Chunk,
    bytes: u64,
}

#[derive(Debug)]
pub struct QueryLocalResultCache {
    max_bytes: u64,
    current_bytes: u64,
    entries: HashMap<QueryLocalResultCacheKey, QueryLocalResultCacheEntry>,
    lru: VecDeque<QueryLocalResultCacheKey>,
    stats: QueryLocalResultCacheStats,
}

impl QueryLocalResultCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            current_bytes: 0,
            entries: HashMap::new(),
            lru: VecDeque::new(),
            stats: QueryLocalResultCacheStats::default(),
        }
    }

    pub fn get(&mut self, key: &QueryLocalResultCacheKey) -> Option<Chunk> {
        let result = self.entries.get(key).map(|entry| entry.result.clone());
        if result.is_some() {
            self.stats.hits = self.stats.hits.saturating_add(1);
            self.touch(key.clone());
        } else {
            self.stats.misses = self.stats.misses.saturating_add(1);
        }
        result
    }

    pub fn insert(&mut self, key: QueryLocalResultCacheKey, result: Chunk, bytes: u64) -> bool {
        if self.max_bytes == 0 || bytes == 0 || bytes > self.max_bytes {
            self.stats.rejected = self.stats.rejected.saturating_add(1);
            return false;
        }

        while self.current_bytes.saturating_add(bytes) > self.max_bytes {
            let Some(evicted_key) = self.lru.pop_front() else {
                break;
            };
            let Some(evicted) = self.entries.remove(&evicted_key) else {
                continue;
            };
            self.current_bytes = self.current_bytes.saturating_sub(evicted.bytes);
            self.stats.evictions = self.stats.evictions.saturating_add(1);
        }

        let entry = QueryLocalResultCacheEntry { result, bytes };
        match self.entries.entry(key.clone()) {
            Entry::Occupied(mut occupied) => {
                self.current_bytes = self.current_bytes.saturating_sub(occupied.get().bytes);
                occupied.insert(entry);
            }
            Entry::Vacant(vacant) => {
                vacant.insert(entry);
            }
        }
        self.touch(key);
        self.current_bytes = self.entries.values().map(|entry| entry.bytes).sum();
        self.stats.admissions = self.stats.admissions.saturating_add(1);
        self.stats.resident_bytes = self.current_bytes;
        true
    }

    pub fn stats(&self) -> QueryLocalResultCacheStats {
        let mut stats = self.stats.clone();
        stats.resident_bytes = self.current_bytes;
        stats
    }

    fn touch(&mut self, key: QueryLocalResultCacheKey) {
        self.lru.retain(|existing| existing != &key);
        self.lru.push_back(key);
    }
}

pub fn digest_chunk_abi_view(
    chunk: &Chunk,
    routine_identities: Vec<RoutineCallIdentity>,
) -> QueryLocalResultCacheKey {
    let mut digest_hasher = DefaultHasher::new();
    let mut null_hasher = DefaultHasher::new();

    chunk.size().hash(&mut digest_hasher);
    chunk.column_count().hash(&mut digest_hasher);

    for column_idx in 0..chunk.column_count() {
        let column = chunk
            .column(column_idx)
            .expect("chunk column should exist for digest");
        column.logical_type().hash(&mut digest_hasher);
        (column.vector_type() as u8).hash(&mut digest_hasher);

        for row_idx in 0..chunk.size() {
            let value = chunk.get_value(column_idx, row_idx);
            value.is_none().hash(&mut null_hasher);
            value.hash(&mut digest_hasher);
        }
    }

    QueryLocalResultCacheKey {
        routine_identities,
        abi_view_digest: finish_hash(digest_hasher.finish()),
        null_pattern: finish_hash(null_hasher.finish()),
    }
}

fn finish_hash(hash: u64) -> String {
    let mut encoded = String::with_capacity(16);
    write!(&mut encoded, "{hash:016x}").expect("formatting digest should succeed");
    encoded
}

#[cfg(test)]
mod tests {
    use super::{digest_chunk_abi_view, QueryLocalResultCache};
    use paro_common::allocator::default_allocator;
    use paro_common::chunk::Chunk;
    use paro_common::types::LogicalType;

    use paro_routine::{BuiltinIntrinsicId, RoutineCallIdentity};
    use std::sync::Arc;

    #[test]
    fn digest_changes_when_values_change() {
        let allocator = Arc::new(default_allocator());
        let left = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3],
                allocator.clone(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let right = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 4],
                allocator,
            )],
            paro_common::test_utils::test_allocator(),
        );

        let left_key = digest_chunk_abi_view(
            &left,
            vec![RoutineCallIdentity::Builtin {
                intrinsic: BuiltinIntrinsicId::Add,
                semantic_tags: Vec::new(),
            }],
        );
        let right_key = digest_chunk_abi_view(
            &right,
            vec![RoutineCallIdentity::Builtin {
                intrinsic: BuiltinIntrinsicId::Add,
                semantic_tags: Vec::new(),
            }],
        );

        assert_ne!(left_key.abi_view_digest, right_key.abi_view_digest);
    }

    #[test]
    fn cache_evicts_when_budget_is_exceeded() {
        let allocator = Arc::new(default_allocator());
        let chunk = Chunk::from_vectors(
            vec![paro_common::test_utils::test_i32_vector_with_allocator(
                &[1, 2, 3],
                allocator.clone(),
            )],
            paro_common::test_utils::test_allocator(),
        );
        let key_a = digest_chunk_abi_view(
            &chunk,
            vec![RoutineCallIdentity::Builtin {
                intrinsic: BuiltinIntrinsicId::Add,
                semantic_tags: Vec::new(),
            }],
        );
        let key_b = digest_chunk_abi_view(
            &Chunk::from_vectors(
                vec![paro_common::test_utils::test_constant_null_with_allocator(
                    LogicalType::Integer,
                    3,
                    allocator,
                )],
                paro_common::test_utils::test_allocator(),
            ),
            vec![RoutineCallIdentity::Builtin {
                intrinsic: BuiltinIntrinsicId::Subtract,
                semantic_tags: Vec::new(),
            }],
        );

        let mut cache = QueryLocalResultCache::new(32);
        assert!(cache.insert(key_a.clone(), chunk.clone(), 24));
        assert!(cache.insert(key_b.clone(), chunk, 24));
        assert!(cache.get(&key_a).is_none());
        assert!(cache.get(&key_b).is_some());
        assert_eq!(cache.stats().evictions, 1);
    }
}
