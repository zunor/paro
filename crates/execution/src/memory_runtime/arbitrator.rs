// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Process-level memory arbitrator for query pools and retained memory.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use paro_common::memory::MemoryResult;
use paro_context::{
    QueryMemoryBudgetSpec, QueryMemoryCoordinator, QueryMemoryRegistration, QueryMemoryTarget,
};

fn saturating_sub_atomic(counter: &AtomicUsize, bytes: usize) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(bytes))
    });
}

#[derive(Debug)]
struct QueryEntry {
    spec: QueryMemoryBudgetSpec,
    target: Weak<dyn QueryMemoryTarget>,
}

#[derive(Debug, Default)]
struct QueryRegistry {
    queries: HashMap<u64, QueryEntry>,
}

/// Process-level memory arbitrator state shared by sessions and query pools.
#[derive(Debug)]
pub struct MemoryArbitrator {
    buffer_pool_limit: AtomicUsize,
    shared_cache_floor: AtomicUsize,
    system_reserve_bytes: AtomicUsize,
    session_retained_bytes: AtomicUsize,
    next_query_id: AtomicU64,
    registry: Mutex<QueryRegistry>,
}

impl MemoryArbitrator {
    pub fn new(buffer_pool_limit: usize) -> Self {
        Self {
            buffer_pool_limit: AtomicUsize::new(buffer_pool_limit),
            shared_cache_floor: AtomicUsize::new(0),
            system_reserve_bytes: AtomicUsize::new(0),
            session_retained_bytes: AtomicUsize::new(0),
            next_query_id: AtomicU64::new(1),
            registry: Mutex::new(QueryRegistry::default()),
        }
    }

    pub fn set_buffer_pool_limit(&self, bytes: usize) {
        self.buffer_pool_limit.store(bytes, Ordering::Release);
        self.refresh_query_capacities();
    }

    pub fn set_shared_cache_floor(&self, bytes: usize) {
        self.shared_cache_floor.store(bytes, Ordering::Release);
        self.refresh_query_capacities();
    }

    pub fn set_system_reserve_bytes(&self, bytes: usize) {
        self.system_reserve_bytes.store(bytes, Ordering::Release);
        self.refresh_query_capacities();
    }

    pub fn add_system_reserve_bytes(&self, bytes: usize) {
        if bytes > 0 {
            self.system_reserve_bytes.fetch_add(bytes, Ordering::AcqRel);
            self.refresh_query_capacities();
        }
    }

    pub fn release_system_reserve_bytes(&self, bytes: usize) {
        if bytes > 0 {
            saturating_sub_atomic(&self.system_reserve_bytes, bytes);
            self.refresh_query_capacities();
        }
    }

    pub fn add_session_retained_bytes(&self, bytes: usize) {
        if bytes > 0 {
            self.session_retained_bytes
                .fetch_add(bytes, Ordering::AcqRel);
            self.refresh_query_capacities();
        }
    }

    pub fn release_session_retained_bytes(&self, bytes: usize) {
        if bytes > 0 {
            saturating_sub_atomic(&self.session_retained_bytes, bytes);
            self.refresh_query_capacities();
        }
    }

    pub fn session_retained_bytes(&self) -> usize {
        self.session_retained_bytes.load(Ordering::Acquire)
    }

    pub fn system_reserve_bytes(&self) -> usize {
        self.system_reserve_bytes.load(Ordering::Acquire)
    }

    pub fn available_for_queries(&self) -> usize {
        self.buffer_pool_limit
            .load(Ordering::Acquire)
            .saturating_sub(self.shared_cache_floor.load(Ordering::Acquire))
            .saturating_sub(self.system_reserve_bytes())
            .saturating_sub(self.session_retained_bytes())
    }

    fn refresh_query_capacities(&self) {
        let mut registry = self
            .registry
            .lock()
            .expect("query memory registry lock poisoned");
        registry
            .queries
            .retain(|_, entry| entry.target.upgrade().is_some());
        let live: Vec<_> = registry
            .queries
            .values()
            .filter_map(|entry| {
                entry
                    .target
                    .upgrade()
                    .map(|target| (entry.spec.clone(), target))
            })
            .collect();
        if live.is_empty() {
            return;
        }

        let shares = compute_fair_shares(self.available_for_queries(), &live);
        for (spec, target) in live {
            let capacity = shares.get(&spec.query_id).copied().unwrap_or(0);
            target.set_capacity_bytes(capacity);
        }
    }

    fn register_target(
        self: Arc<Self>,
        spec: QueryMemoryBudgetSpec,
        target: Weak<dyn QueryMemoryTarget>,
    ) -> QueryMemoryRegistration {
        let query_id = spec.query_id;
        {
            let mut registry = self
                .registry
                .lock()
                .expect("query memory registry lock poisoned");
            registry
                .queries
                .insert(query_id, QueryEntry { spec, target });
        }
        self.refresh_query_capacities();
        let coordinator: Arc<dyn QueryMemoryCoordinator> = self;
        QueryMemoryRegistration::new(coordinator, query_id)
    }

    fn reclaim_from_peers(
        &self,
        requester_query_id: u64,
        target_bytes: usize,
    ) -> MemoryResult<usize> {
        if target_bytes == 0 {
            return Ok(0);
        }

        let mut peers: Vec<_> = {
            let mut registry = self
                .registry
                .lock()
                .expect("query memory registry lock poisoned");
            registry
                .queries
                .retain(|_, entry| entry.target.upgrade().is_some());
            registry
                .queries
                .iter()
                .filter_map(|(query_id, entry)| {
                    if *query_id == requester_query_id {
                        return None;
                    }
                    entry
                        .target
                        .upgrade()
                        .map(|target| (*query_id, target.reclaimable_bytes(), target))
                })
                .collect()
        };
        peers.sort_by(|left, right| right.1.cmp(&left.1));

        let mut reclaimed = 0usize;
        let mut first_error = None;
        for (_, reclaimable, target) in peers {
            if reclaimed >= target_bytes {
                break;
            }
            if reclaimable == 0 {
                continue;
            }
            match target.reclaim(target_bytes - reclaimed) {
                Ok(bytes) => reclaimed = reclaimed.saturating_add(bytes),
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        if reclaimed == 0 {
            if let Some(err) = first_error {
                return Err(err);
            }
        }
        if reclaimed > 0 {
            let requester = {
                let registry = self
                    .registry
                    .lock()
                    .expect("query memory registry lock poisoned");
                registry
                    .queries
                    .get(&requester_query_id)
                    .and_then(|entry| entry.target.upgrade())
            };
            if let Some(requester) = requester {
                let target_capacity = requester
                    .capacity_bytes()
                    .saturating_add(reclaimed)
                    .max(requester.issued_bytes().saturating_add(target_bytes));
                requester.set_capacity_bytes(target_capacity);
            }
        }
        Ok(reclaimed)
    }
}

impl QueryMemoryCoordinator for MemoryArbitrator {
    fn next_query_id(&self) -> u64 {
        self.next_query_id.fetch_add(1, Ordering::AcqRel)
    }

    fn register_query(
        self: Arc<Self>,
        spec: QueryMemoryBudgetSpec,
        target: Weak<dyn QueryMemoryTarget>,
    ) -> QueryMemoryRegistration {
        self.register_target(spec, target)
    }

    fn unregister_query(&self, query_id: u64) {
        {
            let mut registry = self
                .registry
                .lock()
                .expect("query memory registry lock poisoned");
            registry.queries.remove(&query_id);
        }
        self.refresh_query_capacities();
    }

    fn reclaim_for_query(
        &self,
        requester_query_id: u64,
        target_bytes: usize,
    ) -> MemoryResult<usize> {
        self.reclaim_from_peers(requester_query_id, target_bytes)
    }

    fn available_for_queries(&self) -> usize {
        MemoryArbitrator::available_for_queries(self)
    }

    fn session_retained_bytes(&self) -> usize {
        MemoryArbitrator::session_retained_bytes(self)
    }
}

fn compute_fair_shares(
    available_bytes: usize,
    live: &[(QueryMemoryBudgetSpec, Arc<dyn QueryMemoryTarget>)],
) -> HashMap<u64, usize> {
    let mut groups: HashMap<String, Vec<&QueryMemoryBudgetSpec>> = HashMap::new();
    let mut desired_total = 0usize;
    for (spec, _) in live {
        desired_total = desired_total.saturating_add(spec.desired_bytes());
        groups
            .entry(spec.query_group.clone())
            .or_default()
            .push(spec);
    }

    if desired_total <= available_bytes {
        return live
            .iter()
            .map(|(spec, _)| (spec.query_id, spec.desired_bytes()))
            .collect();
    }

    let group_inputs: Vec<_> = groups
        .iter()
        .map(|(group, specs)| {
            let cap = specs
                .iter()
                .fold(0usize, |sum, spec| sum.saturating_add(spec.desired_bytes()));
            let weight = specs.iter().fold(0usize, |sum, spec| {
                sum.saturating_add(spec.priority_weight.max(1))
            });
            (group.clone(), weight.max(1), cap)
        })
        .collect();
    let group_shares = distribute_capped(available_bytes, &group_inputs);

    let mut shares = HashMap::new();
    for (group, specs) in groups {
        let group_share = group_shares.get(&group).copied().unwrap_or(0);
        let query_inputs: Vec<_> = specs
            .iter()
            .map(|spec| {
                (
                    spec.query_id,
                    spec.priority_weight.max(1),
                    spec.desired_bytes(),
                )
            })
            .collect();
        shares.extend(distribute_capped(group_share, &query_inputs));
    }
    shares
}

fn distribute_capped<K>(total: usize, inputs: &[(K, usize, usize)]) -> HashMap<K, usize>
where
    K: Clone + Eq + std::hash::Hash,
{
    let mut shares = HashMap::new();
    if inputs.is_empty() || total == 0 {
        return shares;
    }

    let mut remaining_total = total;
    let mut remaining: Vec<_> = inputs
        .iter()
        .enumerate()
        .filter(|(_, (_, _, cap))| *cap > 0)
        .map(|(idx, _)| idx)
        .collect();

    loop {
        if remaining.is_empty() || remaining_total == 0 {
            break;
        }
        let total_weight = remaining
            .iter()
            .fold(0usize, |sum, idx| sum.saturating_add(inputs[*idx].1.max(1)));
        if total_weight == 0 {
            break;
        }

        let mut capped_any = false;
        let mut next_remaining = Vec::with_capacity(remaining.len());
        for idx in remaining {
            let (_, weight, cap) = &inputs[idx];
            let share = weighted_share(remaining_total, (*weight).max(1), total_weight);
            if share >= *cap {
                shares.insert(inputs[idx].0.clone(), *cap);
                remaining_total = remaining_total.saturating_sub(*cap);
                capped_any = true;
            } else {
                next_remaining.push(idx);
            }
        }

        remaining = next_remaining;
        if !capped_any {
            let mut assigned = 0usize;
            for idx in &remaining {
                let (_, weight, _) = &inputs[*idx];
                let share = weighted_share(remaining_total, (*weight).max(1), total_weight);
                shares.insert(inputs[*idx].0.clone(), share);
                assigned = assigned.saturating_add(share);
            }

            let mut remainder = remaining_total.saturating_sub(assigned);
            for idx in &remaining {
                if remainder == 0 {
                    break;
                }
                let key = inputs[*idx].0.clone();
                let cap = inputs[*idx].2;
                let current = shares.get(&key).copied().unwrap_or(0);
                if current < cap {
                    shares.insert(key, current + 1);
                    remainder -= 1;
                }
            }
            break;
        }
    }

    for (key, _, _) in inputs {
        shares.entry(key.clone()).or_insert(0);
    }
    shares
}

fn weighted_share(total: usize, weight: usize, total_weight: usize) -> usize {
    ((total as u128 * weight as u128) / total_weight.max(1) as u128) as usize
}
