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
    minimum_capacity_bytes: usize,
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

    /// Atomically reserve process memory without violating any admitted query
    /// floor. Dynamic system work must participate in the same admission
    /// boundary as query plans; a class-local limit alone is not sufficient.
    pub fn try_add_system_reserve_bytes(&self, bytes: usize) -> Result<(), usize> {
        if bytes == 0 {
            return Ok(());
        }
        let mut registry = self
            .registry
            .lock()
            .expect("query memory registry lock poisoned");
        registry
            .queries
            .retain(|_, entry| entry.target.upgrade().is_some());
        let query_floor = registry.queries.values().fold(0usize, |total, entry| {
            let issued = entry
                .target
                .upgrade()
                .map(|target| target.issued_bytes())
                .unwrap_or(0);
            total.saturating_add(issued.max(entry.minimum_capacity_bytes))
        });
        let available = self.available_for_queries().saturating_sub(query_floor);
        if bytes > available {
            return Err(available);
        }
        self.system_reserve_bytes.fetch_add(bytes, Ordering::AcqRel);
        let live = registry
            .queries
            .values()
            .filter_map(|entry| {
                entry
                    .target
                    .upgrade()
                    .map(|target| (entry.spec.clone(), entry.minimum_capacity_bytes, target))
            })
            .collect::<Vec<_>>();
        let shares = compute_fair_shares(self.available_for_queries(), &live);
        for (spec, _, target) in live {
            target.set_capacity_bytes(shares.get(&spec.query_id).copied().unwrap_or(0));
        }
        Ok(())
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
                    .map(|target| (entry.spec.clone(), entry.minimum_capacity_bytes, target))
            })
            .collect();
        if live.is_empty() {
            return;
        }

        let shares = compute_fair_shares(self.available_for_queries(), &live);
        for (spec, _, target) in live {
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
            registry.queries.insert(
                query_id,
                QueryEntry {
                    spec,
                    target,
                    minimum_capacity_bytes: 0,
                },
            );
        }
        self.refresh_query_capacities();
        let coordinator: Arc<dyn QueryMemoryCoordinator> = self;
        QueryMemoryRegistration::new(coordinator, query_id)
    }

    fn acquire_capacity_from_peers(
        &self,
        requester_query_id: u64,
        target_bytes: usize,
    ) -> MemoryResult<usize> {
        if target_bytes == 0 {
            return Ok(0);
        }

        let (requester, reclaim_target, mut peers) = {
            let mut registry = self
                .registry
                .lock()
                .expect("query memory registry lock poisoned");
            registry
                .queries
                .retain(|_, entry| entry.target.upgrade().is_some());
            let Some(entry) = registry.queries.get(&requester_query_id) else {
                return Ok(0);
            };
            let Some(requester) = entry.target.upgrade() else {
                return Ok(0);
            };
            let max_capacity = entry.spec.desired_bytes();
            // Peer reclaim redistributes existing capacity. It cannot create
            // headroom beyond the requester's registered hard quota.
            let reclaim_target =
                target_bytes.min(max_capacity.saturating_sub(requester.capacity_bytes()));
            if reclaim_target == 0 {
                return Ok(0);
            }
            let peers: Vec<_> = registry
                .queries
                .iter()
                .filter_map(|(query_id, entry)| {
                    if *query_id == requester_query_id {
                        return None;
                    }
                    entry.target.upgrade().map(|target| {
                        let reclaimable = target.reclaimable_bytes();
                        let unused = target.capacity_bytes().saturating_sub(
                            target.issued_bytes().max(entry.minimum_capacity_bytes),
                        );
                        (
                            *query_id,
                            unused.saturating_add(reclaimable),
                            reclaimable,
                            target,
                        )
                    })
                })
                .collect();
            (requester, reclaim_target, peers)
        };
        peers.sort_by(|left, right| right.1.cmp(&left.1));

        let mut granted = 0usize;
        let mut first_error = None;
        for (peer_query_id, _, reclaimable, target) in peers {
            if granted >= reclaim_target {
                break;
            }
            granted = granted.saturating_add(self.transfer_unused_capacity(
                requester_query_id,
                &requester,
                peer_query_id,
                &target,
                reclaim_target - granted,
            ));
            if granted >= reclaim_target || reclaimable == 0 {
                continue;
            }

            match target.reclaim(reclaim_target - granted) {
                Ok(bytes) => {
                    granted = granted.saturating_add(self.transfer_unused_capacity(
                        requester_query_id,
                        &requester,
                        peer_query_id,
                        &target,
                        bytes.min(reclaim_target - granted),
                    ));
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        if granted == 0 {
            if let Some(err) = first_error {
                return Err(err);
            }
        }
        Ok(granted)
    }

    fn transfer_unused_capacity(
        &self,
        requester_query_id: u64,
        requester: &Arc<dyn QueryMemoryTarget>,
        peer_query_id: u64,
        peer: &Arc<dyn QueryMemoryTarget>,
        target_bytes: usize,
    ) -> usize {
        if target_bytes == 0 {
            return 0;
        }

        // Capacity recomputation and peer transfers share the registry lock.
        // Revalidate both registrations after running the peer's reclaimer so
        // an unregistered target cannot donate capacity that was redistributed
        // by a concurrent refresh.
        let registry = self
            .registry
            .lock()
            .expect("query memory registry lock poisoned");
        let Some((max_capacity, registered_requester)) =
            registry.queries.get(&requester_query_id).and_then(|entry| {
                entry
                    .target
                    .upgrade()
                    .map(|target| (entry.spec.desired_bytes(), target))
            })
        else {
            return 0;
        };
        if !Arc::ptr_eq(&registered_requester, requester) {
            return 0;
        }
        let Some((registered_peer, peer_minimum)) =
            registry.queries.get(&peer_query_id).and_then(|entry| {
                entry
                    .target
                    .upgrade()
                    .map(|target| (target, entry.minimum_capacity_bytes))
            })
        else {
            return 0;
        };
        if !Arc::ptr_eq(&registered_peer, peer) {
            return 0;
        }

        let requester_headroom = max_capacity.saturating_sub(requester.capacity_bytes());
        let peer_excess = peer
            .capacity_bytes()
            .saturating_sub(peer.issued_bytes().max(peer_minimum));
        let transferable = target_bytes.min(requester_headroom).min(peer_excess);
        let relinquished = peer.relinquish_unused_capacity(transferable);
        requester.grant_capacity(relinquished, max_capacity)
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

    fn request_additional_capacity(
        &self,
        requester_query_id: u64,
        target_bytes: usize,
    ) -> MemoryResult<usize> {
        self.acquire_capacity_from_peers(requester_query_id, target_bytes)
    }

    fn try_reserve_minimum_capacity(
        &self,
        query_id: u64,
        minimum_bytes: usize,
    ) -> MemoryResult<bool> {
        // A reservation is a transaction over live ownership, not just the
        // requested floor metadata.  First reclaim peers synchronously when
        // their issued bytes would otherwise make the invariant impossible;
        // only then publish the new floor under the registry lock.
        let peers = {
            let mut registry = self
                .registry
                .lock()
                .expect("query memory registry lock poisoned");
            registry
                .queries
                .retain(|_, entry| entry.target.upgrade().is_some());
            let Some(entry) = registry.queries.get(&query_id) else {
                return Ok(false);
            };
            if minimum_bytes > entry.spec.desired_bytes() {
                return Ok(false);
            }
            let effective_total = registry.queries.iter().fold(0usize, |total, (id, entry)| {
                let issued = entry
                    .target
                    .upgrade()
                    .map(|target| target.issued_bytes())
                    .unwrap_or(0);
                let floor = if *id == query_id {
                    minimum_bytes
                } else {
                    entry.minimum_capacity_bytes
                };
                total.saturating_add(issued.max(floor))
            });
            let deficit = effective_total.saturating_sub(self.available_for_queries());
            if deficit == 0 {
                Vec::new()
            } else {
                let mut peers = registry
                    .queries
                    .iter()
                    .filter_map(|(id, entry)| {
                        (*id != query_id).then(|| {
                            entry.target.upgrade().map(|target| {
                                let reclaimable = target.reclaimable_bytes().min(
                                    target
                                        .issued_bytes()
                                        .saturating_sub(entry.minimum_capacity_bytes),
                                );
                                (reclaimable, target)
                            })
                        })?
                    })
                    .filter(|(reclaimable, _)| *reclaimable > 0)
                    .collect::<Vec<_>>();
                peers.sort_by(|left, right| right.0.cmp(&left.0));
                peers
            }
        };

        let mut remaining = {
            let registry = self
                .registry
                .lock()
                .expect("query memory registry lock poisoned");
            effective_floor_total(&registry, Some((query_id, minimum_bytes)))
                .saturating_sub(self.available_for_queries())
        };
        for (reclaimable, peer) in peers {
            if remaining == 0 {
                break;
            }
            let reclaimed = peer.reclaim(remaining.min(reclaimable))?;
            remaining = remaining.saturating_sub(reclaimed);
        }

        let mut registry = self
            .registry
            .lock()
            .expect("query memory registry lock poisoned");
        registry
            .queries
            .retain(|_, entry| entry.target.upgrade().is_some());
        let Some(entry) = registry.queries.get(&query_id) else {
            return Ok(false);
        };
        if minimum_bytes > entry.spec.desired_bytes()
            || effective_floor_total(&registry, Some((query_id, minimum_bytes)))
                > self.available_for_queries()
        {
            return Ok(false);
        }
        registry
            .queries
            .get_mut(&query_id)
            .expect("validated query registration disappeared")
            .minimum_capacity_bytes = minimum_bytes;
        let live = registry
            .queries
            .values()
            .filter_map(|entry| {
                entry
                    .target
                    .upgrade()
                    .map(|target| (entry.spec.clone(), entry.minimum_capacity_bytes, target))
            })
            .collect::<Vec<_>>();

        let shares = compute_fair_shares(self.available_for_queries(), &live);
        for (spec, _, target) in live {
            target.set_capacity_bytes(shares.get(&spec.query_id).copied().unwrap_or(0));
        }
        Ok(true)
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
    live: &[(QueryMemoryBudgetSpec, usize, Arc<dyn QueryMemoryTarget>)],
) -> HashMap<u64, usize> {
    let mut groups: HashMap<String, Vec<&QueryMemoryBudgetSpec>> = HashMap::new();
    let mut floors = HashMap::new();
    let mut floor_total = 0usize;
    let mut residual_total = 0usize;
    for (spec, floor, target) in live {
        let issued = target.issued_bytes();
        let floor = (*floor).max(issued).min(spec.desired_bytes());
        floors.insert(spec.query_id, floor);
        floor_total = floor_total.saturating_add(floor);
        residual_total = residual_total.saturating_add(spec.desired_bytes().saturating_sub(floor));
        groups
            .entry(spec.query_group.clone())
            .or_default()
            .push(spec);
    }

    let residual_available = available_bytes.saturating_sub(floor_total);
    if residual_total <= residual_available {
        return live
            .iter()
            .map(|(spec, _, _)| (spec.query_id, spec.desired_bytes()))
            .collect();
    }

    let group_inputs: Vec<_> = groups
        .iter()
        .map(|(group, specs)| {
            let cap = specs.iter().fold(0usize, |sum, spec| {
                sum.saturating_add(
                    spec.desired_bytes()
                        .saturating_sub(floors.get(&spec.query_id).copied().unwrap_or(0)),
                )
            });
            let weight = specs.iter().fold(0usize, |sum, spec| {
                sum.saturating_add(spec.priority_weight.max(1))
            });
            (group.clone(), weight.max(1), cap)
        })
        .collect();
    let group_shares = distribute_capped(residual_available, &group_inputs);

    let mut shares = HashMap::new();
    for (group, specs) in groups {
        let group_share = group_shares.get(&group).copied().unwrap_or(0);
        let query_inputs: Vec<_> = specs
            .iter()
            .map(|spec| {
                (
                    spec.query_id,
                    spec.priority_weight.max(1),
                    spec.desired_bytes()
                        .saturating_sub(floors.get(&spec.query_id).copied().unwrap_or(0)),
                )
            })
            .collect();
        for (query_id, residual) in distribute_capped(group_share, &query_inputs) {
            shares.insert(
                query_id,
                floors
                    .get(&query_id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(residual),
            );
        }
    }
    for (query_id, floor) in floors {
        shares.entry(query_id).or_insert(floor);
    }
    shares
}

fn effective_floor_total(registry: &QueryRegistry, replacement: Option<(u64, usize)>) -> usize {
    registry.queries.iter().fold(0usize, |total, (id, entry)| {
        let issued = entry
            .target
            .upgrade()
            .map(|target| target.issued_bytes())
            .unwrap_or(0);
        let floor = replacement
            .filter(|(replacement_id, _)| replacement_id == id)
            .map(|(_, floor)| floor)
            .unwrap_or(entry.minimum_capacity_bytes);
        total.saturating_add(issued.max(floor))
    })
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
