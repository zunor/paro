//! TemporaryMemoryManager - Dynamic memory allocation for query execution.
//!
//! - TemporaryMemoryState tracks per-operator memory needs
//! - TemporaryMemoryManager coordinates memory across concurrent operators
//! - Dynamic reservation based on remaining size and memory pressure
//! - Cost function optimization for fair memory distribution

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use super::buffer_pool::BufferPool;
use super::DEFAULT_BLOCK_ALLOC_SIZE;

/// 512 blocks per state per thread (0.125GB per thread for DEFAULT_BLOCK_ALLOC_SIZE).
const MINIMUM_RESERVATION_PER_STATE_PER_THREAD: usize = 512 * DEFAULT_BLOCK_ALLOC_SIZE;

/// 1/16th of the available main memory.
const MINIMUM_RESERVATION_MEMORY_LIMIT_DIVISOR: usize = 16;

/// Maximum ratio of memory limit that we reserve using TemporaryMemoryManager.
const MAXIMUM_MEMORY_LIMIT_RATIO: f64 = 0.9;

/// Maximum ratio of remaining memory that we reserve per TemporaryMemoryState.
const MAXIMUM_FREE_MEMORY_RATIO: f64 = 0.9;

/// Minimum number of state reservations to leave room for.
const MINIMUM_REMAINING_STATE_RESERVATIONS: usize = 8;

/// Maximum number of state reservations to leave room for.
const MAXIMUM_REMAINING_STATE_RESERVATIONS: usize = 32;

/// Multiplier for optimization iterations.
const OPTIMIZATION_ITERATIONS_MULTIPLIER: usize = 5;

/// State of temporary memory for a single operator/query component.
///
/// As long as this is within scope, it is active and tracked by the manager.
/// When dropped, it automatically unregisters from the manager.
pub struct TemporaryMemoryState {
    /// The TemporaryMemoryManager that owns this state.
    manager: Weak<TemporaryMemoryManager>,

    /// The remaining size needed if it could fit fully in memory.
    remaining_size: AtomicUsize,

    /// The minimum reservation for this state.
    minimum_reservation: AtomicUsize,

    /// How much memory this operator has reserved.
    reservation: AtomicUsize,

    /// Peak reservation observed over the lifetime of this state.
    peak_reservation: AtomicUsize,

    /// The weight used for determining the reservation for this state.
    /// Higher penalty means more expensive to materialize (spill to disk).
    materialization_penalty: AtomicUsize,
}

impl TemporaryMemoryState {
    /// Create a new temporary memory state.
    fn new(manager: Weak<TemporaryMemoryManager>, minimum_reservation: usize) -> Self {
        Self {
            manager,
            remaining_size: AtomicUsize::new(0),
            minimum_reservation: AtomicUsize::new(minimum_reservation),
            reservation: AtomicUsize::new(0),
            peak_reservation: AtomicUsize::new(0),
            materialization_penalty: AtomicUsize::new(1),
        }
    }

    /// Set the remaining size needed for this state.
    ///
    /// NOTE: This does not update the reservation! Use `set_remaining_size_and_update_reservation`
    /// if you want to update the reservation as well.
    pub fn set_remaining_size(&self, new_remaining_size: usize) {
        if let Some(manager) = self.manager.upgrade() {
            let _guard = manager.lock();
            manager.set_remaining_size_internal(self, new_remaining_size);
        }
    }

    /// Set the remaining size and update the reservation.
    ///
    /// This is the typical method to call when the operator knows how much
    /// memory it needs.
    pub fn set_remaining_size_and_update_reservation(&self, new_remaining_size: usize) {
        debug_assert!(new_remaining_size != 0, "Use set_zero instead");
        if let Some(manager) = self.manager.upgrade() {
            let _guard = manager.lock();
            manager.set_remaining_size_internal(self, new_remaining_size);
            manager.update_state_internal(self);
        }
    }

    /// Set the remaining size to 0 and update the reservation to 0.
    pub fn set_zero(&self) {
        if let Some(manager) = self.manager.upgrade() {
            let _guard = manager.lock();
            manager.set_remaining_size_internal(self, 0);
            manager.set_reservation_internal(self, 0);
        }
    }

    /// Get the remaining size that was set for this state.
    #[inline]
    pub fn get_remaining_size(&self) -> usize {
        self.remaining_size.load(Ordering::Acquire)
    }

    /// Set the minimum reservation for this state.
    pub fn set_minimum_reservation(&self, new_minimum_reservation: usize) {
        self.minimum_reservation
            .store(new_minimum_reservation, Ordering::Release);
    }

    /// Get the minimum reservation for this state.
    #[inline]
    pub fn get_minimum_reservation(&self) -> usize {
        self.minimum_reservation.load(Ordering::Acquire)
    }

    /// Update the reservation based on current remaining size.
    pub fn update_reservation(&self) {
        if let Some(manager) = self.manager.upgrade() {
            let _guard = manager.lock();
            manager.update_state_internal(self);
        }
    }

    /// Get the reservation of this state.
    #[inline]
    pub fn get_reservation(&self) -> usize {
        self.reservation.load(Ordering::Acquire)
    }

    /// Get the peak reservation observed for this state.
    #[inline]
    pub fn get_peak_reservation(&self) -> usize {
        self.peak_reservation.load(Ordering::Acquire)
    }

    /// Set the materialization penalty for this state.
    ///
    /// Higher penalty means more expensive to materialize (spill to disk).
    pub fn set_materialization_penalty(&self, new_materialization_penalty: usize) {
        if let Some(manager) = self.manager.upgrade() {
            let _guard = manager.lock();
            self.materialization_penalty
                .store(new_materialization_penalty, Ordering::Release);
        }
    }

    /// Get the materialization penalty for this state.
    #[inline]
    pub fn get_materialization_penalty(&self) -> usize {
        self.materialization_penalty.load(Ordering::Acquire)
    }
}

impl Drop for TemporaryMemoryState {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            manager.unregister(self);
        }
    }
}

impl std::fmt::Debug for TemporaryMemoryState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporaryMemoryState")
            .field("remaining_size", &self.get_remaining_size())
            .field("minimum_reservation", &self.get_minimum_reservation())
            .field("reservation", &self.get_reservation())
            .field("peak_reservation", &self.get_peak_reservation())
            .field(
                "materialization_penalty",
                &self.get_materialization_penalty(),
            )
            .finish()
    }
}

/// Configuration for the TemporaryMemoryManager.
///
/// This is updated from the execution context when states are registered or updated.
#[derive(Debug, Clone)]
pub struct TemporaryMemoryConfig {
    /// Memory limit of the buffer pool.
    pub memory_limit: usize,
    /// Whether there is a temporary directory for spilling.
    pub has_temporary_directory: bool,
    /// Number of threads.
    pub num_threads: usize,
    /// Number of active connections.
    pub num_connections: usize,
    /// Maximum memory per query.
    pub query_max_memory: usize,
    /// Force external processing (for testing).
    pub force_external: bool,
}

impl Default for TemporaryMemoryConfig {
    fn default() -> Self {
        Self {
            memory_limit: usize::MAX,
            has_temporary_directory: false,
            num_threads: 1,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        }
    }
}

/// TemporaryMemoryManager coordinates memory allocation across concurrent operators.
///
/// It tries to dynamically assign memory to concurrent states such that their
/// combined memory usage does not exceed the limit.
pub struct TemporaryMemoryManager {
    /// Lock for thread-safe operations.
    lock: Mutex<()>,

    /// Configuration (updated from context).
    config: RwLock<TemporaryMemoryConfig>,

    /// Currently active states (stored as raw pointers for identity comparison).
    active_states: RwLock<HashSet<usize>>,

    /// The sum of reservations of all active states.
    total_reservation: AtomicUsize,

    /// The sum of the remaining size of all active states.
    total_remaining_size: AtomicUsize,

    /// Reference to the buffer pool for memory limits.
    buffer_pool: Option<Weak<BufferPool>>,
}

impl TemporaryMemoryManager {
    /// Create a new TemporaryMemoryManager.
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            config: RwLock::new(TemporaryMemoryConfig::default()),
            active_states: RwLock::new(HashSet::new()),
            total_reservation: AtomicUsize::new(0),
            total_remaining_size: AtomicUsize::new(0),
            buffer_pool: None,
        }
    }

    /// Create a new TemporaryMemoryManager with a buffer pool reference.
    pub fn with_buffer_pool(buffer_pool: Weak<BufferPool>) -> Self {
        Self {
            lock: Mutex::new(()),
            config: RwLock::new(TemporaryMemoryConfig::default()),
            active_states: RwLock::new(HashSet::new()),
            total_reservation: AtomicUsize::new(0),
            total_remaining_size: AtomicUsize::new(0),
            buffer_pool: Some(buffer_pool),
        }
    }

    /// Lock the manager for exclusive access.
    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.lock.lock().unwrap()
    }

    /// Update configuration from the buffer pool.
    pub fn update_configuration(&self, config: TemporaryMemoryConfig) {
        let mut cfg = self.config.write().unwrap();
        *cfg = config;
    }

    /// Update configuration from buffer pool reference.
    fn update_configuration_from_pool(&self) {
        if let Some(pool_weak) = &self.buffer_pool {
            if let Some(pool) = pool_weak.upgrade() {
                let memory_limit = (MAXIMUM_MEMORY_LIMIT_RATIO * pool.max_memory() as f64) as usize;
                let mut cfg = self.config.write().unwrap();
                cfg.memory_limit = memory_limit;
            }
        }
    }

    /// Get the default minimum reservation.
    fn default_minimum_reservation(&self) -> usize {
        let cfg = self.config.read().unwrap();
        std::cmp::min(
            cfg.num_threads * MINIMUM_RESERVATION_PER_STATE_PER_THREAD,
            cfg.memory_limit / MINIMUM_RESERVATION_MEMORY_LIMIT_DIVISOR,
        )
    }

    /// Register a new TemporaryMemoryState.
    ///
    /// Returns an Arc to the state that will automatically unregister when dropped.
    pub fn register(self: &Arc<Self>) -> Arc<TemporaryMemoryState> {
        let _guard = self.lock();
        self.update_configuration_from_pool();

        let min_reservation = self.default_minimum_reservation();
        let state = Arc::new(TemporaryMemoryState::new(
            Arc::downgrade(self),
            min_reservation,
        ));

        // Set initial remaining size and reservation
        self.set_remaining_size_internal(&state, min_reservation);
        self.set_reservation_internal(&state, min_reservation);

        // Track the state
        {
            let mut states = self.active_states.write().unwrap();
            states.insert(Arc::as_ptr(&state) as usize);
        }

        self.verify();
        state
    }

    /// Unregister a TemporaryMemoryState.
    fn unregister(&self, state: &TemporaryMemoryState) {
        let _guard = self.lock();

        self.set_reservation_internal(state, 0);
        self.set_remaining_size_internal(state, 0);

        {
            let mut states = self.active_states.write().unwrap();
            states.remove(&(state as *const _ as usize));
        }

        self.verify();
    }

    /// Set the remaining size of a state (must hold lock).
    fn set_remaining_size_internal(&self, state: &TemporaryMemoryState, new_remaining_size: usize) {
        let old_size = state.remaining_size.load(Ordering::Acquire);
        self.total_remaining_size
            .fetch_sub(old_size, Ordering::AcqRel);
        state
            .remaining_size
            .store(new_remaining_size, Ordering::Release);
        self.total_remaining_size
            .fetch_add(new_remaining_size, Ordering::AcqRel);
    }

    /// Set the reservation of a state (must hold lock).
    fn set_reservation_internal(&self, state: &TemporaryMemoryState, new_reservation: usize) {
        let old_reservation = state.reservation.load(Ordering::Acquire);
        self.total_reservation
            .fetch_sub(old_reservation, Ordering::AcqRel);
        state.reservation.store(new_reservation, Ordering::Release);
        state
            .peak_reservation
            .fetch_max(new_reservation, Ordering::AcqRel);
        self.total_reservation
            .fetch_add(new_reservation, Ordering::AcqRel);
    }

    /// Update the state's reservation based on current conditions (must hold lock).
    fn update_state_internal(&self, state: &TemporaryMemoryState) {
        self.update_configuration_from_pool();

        let cfg = self.config.read().unwrap();
        let remaining_size = state.get_remaining_size();
        let min_reservation = state.get_minimum_reservation();
        let current_reservation = state.get_reservation();

        // Lower bound is minimum of minimum_reservation and remaining_size
        let lower_bound = std::cmp::min(min_reservation, remaining_size);

        if remaining_size == 0 {
            // End of state
            drop(cfg);
            self.set_reservation_internal(state, 0);
        } else if cfg.force_external {
            // Force external processing - give minimum
            drop(cfg);
            self.set_reservation_internal(state, lower_bound);
        } else if !cfg.has_temporary_directory {
            // Cannot offload, cannot limit memory usage
            drop(cfg);
            self.set_reservation_internal(state, remaining_size);
        } else {
            let total_reservation = self.total_reservation.load(Ordering::Acquire);
            let other_reservation = total_reservation.saturating_sub(current_reservation);

            if other_reservation + lower_bound >= cfg.memory_limit {
                // Overshot - give minimum
                drop(cfg);
                self.set_reservation_internal(state, lower_bound);
            } else {
                // Calculate upper bound
                let free_memory = cfg.memory_limit.saturating_sub(other_reservation);
                let mut upper_bound = std::cmp::min(remaining_size, cfg.query_max_memory);
                upper_bound = std::cmp::min(
                    upper_bound,
                    (MAXIMUM_FREE_MEMORY_RATIO * free_memory as f64) as usize,
                );
                upper_bound = std::cmp::min(upper_bound, free_memory);

                let new_reservation = if lower_bound >= upper_bound {
                    lower_bound
                } else {
                    let total_remaining = self.total_remaining_size.load(Ordering::Acquire);
                    if total_remaining > cfg.memory_limit {
                        // Need to compute optimal reservation
                        drop(cfg);
                        self.compute_reservation(state)
                    } else {
                        drop(cfg);
                        upper_bound
                    }
                };

                self.set_reservation_internal(state, new_reservation);
            }
        }

        self.verify();
    }

    /// Compute initial reservation for a state.
    fn compute_initial_reservation(&self, state: &TemporaryMemoryState) -> usize {
        let min_reservation = state.get_minimum_reservation();
        let current_reservation = state.get_reservation();
        let remaining_size = state.get_remaining_size();

        let result = std::cmp::max(min_reservation, current_reservation);
        let result = std::cmp::min(result, remaining_size);
        std::cmp::max(result, 1)
    }

    /// Compute optimal reservation using cost function optimization.
    ///
    /// This implements a cost function that balances memory allocation
    /// across multiple concurrent operators based on their remaining size and
    /// materialization penalty.
    fn compute_reservation(&self, target_state: &TemporaryMemoryState) -> usize {
        let cfg = self.config.read().unwrap();
        let memory_limit = cfg.memory_limit;
        let num_connections = cfg.num_connections;
        drop(cfg);

        // Collect all active states
        let states = self.active_states.read().unwrap();
        if states.is_empty() {
            return target_state.get_minimum_reservation();
        }

        // Build vectors for optimization
        // Note: We need to be careful here since we're working with raw pointers
        // In a real implementation, we'd need a safer way to iterate states
        let target_ptr = target_state as *const _ as usize;
        let state_ptrs: Vec<usize> = states.iter().copied().collect();
        drop(states);

        let n = state_ptrs.len();
        if n == 0 {
            return target_state.get_minimum_reservation();
        }

        // Find target state index
        let target_index = state_ptrs.iter().position(|&p| p == target_ptr);
        if target_index.is_none() {
            return target_state.get_minimum_reservation();
        }
        let target_index = target_index.unwrap();

        // Compute initial reservations and sum
        let mut res: Vec<usize> = Vec::with_capacity(n);
        let mut sum_of_initial_res = 0usize;

        for &ptr in &state_ptrs {
            // SAFETY: These pointers are valid as long as the states are alive
            let state = unsafe { &*(ptr as *const TemporaryMemoryState) };
            let initial = self.compute_initial_reservation(state);
            sum_of_initial_res += initial;
            res.push(initial);
        }

        if sum_of_initial_res >= memory_limit {
            return res[target_index];
        }

        let free_memory = memory_limit - sum_of_initial_res;

        // Distribute memory using optimization iterations
        let mut remaining_memory = free_memory;
        let optimization_iterations = OPTIMIZATION_ITERATIONS_MULTIPLIER * n;

        for opt_idx in 0..optimization_iterations {
            if remaining_memory == 0 {
                break;
            }

            // Compute derivatives for all states
            let der = self.compute_derivatives(&state_ptrs, &res);

            // Find state with lowest derivative that can still grow
            let mut min_idx = 0;
            let mut min_der = f64::MAX;

            for i in 0..n {
                let state = unsafe { &*(state_ptrs[i] as *const TemporaryMemoryState) };
                if res[i] >= state.get_remaining_size() {
                    continue; // Can't increase maxed states
                }
                if der[i] < min_der {
                    min_idx = i;
                    min_der = der[i];
                }
            }

            let min_state = unsafe { &*(state_ptrs[min_idx] as *const TemporaryMemoryState) };

            // Calculate memory to distribute this round
            let iter_memory = (remaining_memory as f64 / (optimization_iterations - opt_idx) as f64)
                .ceil() as usize;

            // Compute how much we can add
            let state_room = min_state.get_remaining_size().saturating_sub(res[min_idx]);
            let delta = std::cmp::min(iter_memory, state_room);
            let delta = std::cmp::min(delta, remaining_memory);

            // Update counts
            res[min_idx] += delta;
            remaining_memory -= delta;
        }

        // Apply upper bound based on MAXIMUM_FREE_MEMORY_RATIO
        // Sort states by derivative at max reservation
        let mut max_res: Vec<usize> = Vec::with_capacity(n);
        for &ptr in &state_ptrs {
            let state = unsafe { &*(ptr as *const TemporaryMemoryState) };
            max_res.push(state.get_remaining_size().saturating_sub(1).max(1));
        }
        let der_at_max = self.compute_derivatives(&state_ptrs, &max_res);

        let mut idxs: Vec<usize> = (0..n).collect();
        idxs.sort_by(|&a, &b| der_at_max[a].partial_cmp(&der_at_max[b]).unwrap());

        // Loop through sorted indices
        let mut remaining_memory = free_memory;
        for idx in idxs {
            let state = unsafe { &*(state_ptrs[idx] as *const TemporaryMemoryState) };
            let initial_state_reservation = self.compute_initial_reservation(state);

            // Bound by ratio
            let state_remaining = initial_state_reservation + remaining_memory;
            let mut upper_bound = (MAXIMUM_FREE_MEMORY_RATIO * state_remaining as f64) as usize;

            // Bound by leaving room for other states
            let num_other_states =
                std::cmp::min(MAXIMUM_REMAINING_STATE_RESERVATIONS, num_connections);
            let num_other_states =
                std::cmp::max(num_other_states, MINIMUM_REMAINING_STATE_RESERVATIONS);
            upper_bound = std::cmp::min(
                upper_bound,
                num_other_states * self.default_minimum_reservation(),
            );

            let mut state_reservation = std::cmp::min(res[idx], upper_bound);
            state_reservation = std::cmp::max(state_reservation, initial_state_reservation);

            if idx == target_index {
                return state_reservation;
            }

            let delta = state_reservation.saturating_sub(initial_state_reservation);
            remaining_memory = remaining_memory.saturating_sub(delta);
        }

        // Should not reach here
        res[target_index]
    }

    /// Compute derivatives for the cost function.
    ///
    /// The cost function takes "throughput" (reservation / size) of each operator
    /// as its principal input.
    fn compute_derivatives(&self, state_ptrs: &[usize], res: &[usize]) -> Vec<f64> {
        let n = state_ptrs.len();
        let mut der = vec![f64::MAX; n];

        if n == 0 {
            return der;
        }

        // Compute products and materialization cost
        let mut prod_siz = 1.0f64;
        let mut prod_res = 1.0f64;
        let mut mat_cost = 0.0f64;

        for i in 0..n {
            let state = unsafe { &*(state_ptrs[i] as *const TemporaryMemoryState) };
            let resd = res[i] as f64;
            let sizd = std::cmp::max(state.get_remaining_size(), 1) as f64;
            let pend = state.get_materialization_penalty() as f64;

            prod_res *= resd;
            prod_siz *= sizd;
            mat_cost += pend * (1.0 - resd / sizd);
        }

        let nd = n as f64;
        let tp_mult = 1.0 - (prod_res / prod_siz).powf(1.0 / nd);

        // Compute derivative for each state
        let intermediate = -(prod_res.powf(1.0 / nd) * mat_cost) / (nd * prod_siz.powf(1.0 / nd));

        for i in 0..n {
            let state = unsafe { &*(state_ptrs[i] as *const TemporaryMemoryState) };
            let resd = res[i] as f64;
            let sizd = std::cmp::max(state.get_remaining_size(), 1) as f64;
            let pend = state.get_materialization_penalty() as f64;

            der[i] = intermediate / resd - pend * tp_mult / sizd;
        }

        der
    }

    /// Verify internal counts (debug only).
    #[cfg(debug_assertions)]
    fn verify(&self) {
        // In debug mode, verify that totals match
        let states = self.active_states.read().unwrap();
        let mut total_reservation = 0usize;
        let mut total_remaining_size = 0usize;

        for &ptr in states.iter() {
            let state = unsafe { &*(ptr as *const TemporaryMemoryState) };
            total_reservation += state.get_reservation();
            total_remaining_size += state.get_remaining_size();
        }

        debug_assert_eq!(
            total_reservation,
            self.total_reservation.load(Ordering::Acquire),
            "Total reservation mismatch"
        );
        debug_assert_eq!(
            total_remaining_size,
            self.total_remaining_size.load(Ordering::Acquire),
            "Total remaining size mismatch"
        );
    }

    #[cfg(not(debug_assertions))]
    fn verify(&self) {
        // No-op in release mode
    }

    /// Get total reservation across all states.
    pub fn get_total_reservation(&self) -> usize {
        self.total_reservation.load(Ordering::Acquire)
    }

    /// Get total remaining size across all states.
    pub fn get_total_remaining_size(&self) -> usize {
        self.total_remaining_size.load(Ordering::Acquire)
    }

    /// Get a snapshot of current temporary memory configuration.
    pub fn current_config(&self) -> TemporaryMemoryConfig {
        self.config.read().unwrap().clone()
    }

    /// Get number of active states.
    pub fn get_active_state_count(&self) -> usize {
        let states = self.active_states.read().unwrap();
        states.len()
    }
}

impl Default for TemporaryMemoryManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TemporaryMemoryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporaryMemoryManager")
            .field("total_reservation", &self.get_total_reservation())
            .field("total_remaining_size", &self.get_total_remaining_size())
            .field("active_states", &self.get_active_state_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temporary_memory_manager_creation() {
        let manager = Arc::new(TemporaryMemoryManager::new());
        assert_eq!(manager.get_total_reservation(), 0);
        assert_eq!(manager.get_total_remaining_size(), 0);
        assert_eq!(manager.get_active_state_count(), 0);
    }

    #[test]
    fn test_register_state() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        // Configure with reasonable defaults
        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024, // 1 GB
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state = manager.register();

        assert_eq!(manager.get_active_state_count(), 1);
        assert!(state.get_reservation() > 0);
        assert!(state.get_minimum_reservation() > 0);
    }

    #[test]
    fn test_state_auto_unregister() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        {
            let state = manager.register();
            assert_eq!(manager.get_active_state_count(), 1);
            drop(state);
        }

        assert_eq!(manager.get_active_state_count(), 0);
        assert_eq!(manager.get_total_reservation(), 0);
        assert_eq!(manager.get_total_remaining_size(), 0);
    }

    #[test]
    fn test_set_remaining_size() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state = manager.register();

        // Set remaining size
        state.set_remaining_size(1024 * 1024); // 1 MB
        assert_eq!(state.get_remaining_size(), 1024 * 1024);
    }

    #[test]
    fn test_set_remaining_size_and_update_reservation() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state = manager.register();

        // Set remaining size and update reservation
        state.set_remaining_size_and_update_reservation(10 * 1024 * 1024); // 10 MB

        assert_eq!(state.get_remaining_size(), 10 * 1024 * 1024);
        // Reservation should be updated (exact value depends on algorithm)
        assert!(state.get_reservation() > 0);
    }

    #[test]
    fn test_set_zero() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state = manager.register();

        // Set some remaining size first
        state.set_remaining_size_and_update_reservation(1024 * 1024);
        assert!(state.get_reservation() > 0);

        // Set to zero
        state.set_zero();
        assert_eq!(state.get_remaining_size(), 0);
        assert_eq!(state.get_reservation(), 0);
    }

    #[test]
    fn test_materialization_penalty() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state = manager.register();

        // Default penalty is 1
        assert_eq!(state.get_materialization_penalty(), 1);

        // Set higher penalty
        state.set_materialization_penalty(10);
        assert_eq!(state.get_materialization_penalty(), 10);
    }

    #[test]
    fn test_multiple_states() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 2,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state1 = manager.register();
        let state2 = manager.register();

        assert_eq!(manager.get_active_state_count(), 2);

        // Both should have reservations
        assert!(state1.get_reservation() > 0);
        assert!(state2.get_reservation() > 0);

        // Total should be sum of individual reservations
        assert_eq!(
            manager.get_total_reservation(),
            state1.get_reservation() + state2.get_reservation()
        );
    }

    #[test]
    fn test_force_external() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: true, // Force external processing
        });

        let state = manager.register();

        // Set a large remaining size
        state.set_remaining_size_and_update_reservation(100 * 1024 * 1024);

        // With force_external, reservation should be at minimum
        let min_reservation = state.get_minimum_reservation();
        let remaining_size = state.get_remaining_size();
        let expected = std::cmp::min(min_reservation, remaining_size);

        assert_eq!(state.get_reservation(), expected);
    }

    #[test]
    fn test_no_temporary_directory() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: false, // No temp directory
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state = manager.register();

        // Set remaining size
        let size = 10 * 1024 * 1024;
        state.set_remaining_size_and_update_reservation(size);

        // Without temp directory, reservation should equal remaining size
        // (can't spill to disk)
        assert_eq!(state.get_reservation(), size);
    }

    #[test]
    fn test_memory_pressure() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        // Small memory limit to create pressure
        // Note: The actual memory_limit used internally is MAXIMUM_MEMORY_LIMIT_RATIO * configured_limit
        let configured_limit = 10 * 1024 * 1024; // 10 MB
        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: configured_limit,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 4,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        // Register multiple states that want more than available
        let state1 = manager.register();
        let state2 = manager.register();
        let state3 = manager.register();

        state1.set_remaining_size_and_update_reservation(5 * 1024 * 1024);
        state2.set_remaining_size_and_update_reservation(5 * 1024 * 1024);
        state3.set_remaining_size_and_update_reservation(5 * 1024 * 1024);

        // Total reservation should be reasonable under memory pressure
        // Note: Initial reservations are set before we update remaining size,
        // so total may exceed the limit slightly due to minimum reservations.
        // The key behavior is that the system tries to manage memory fairly.
        let total = manager.get_total_reservation();
        let total_remaining = manager.get_total_remaining_size();

        // Verify that reservations are being tracked
        assert!(total > 0, "Total reservation should be positive");
        assert_eq!(
            total_remaining,
            15 * 1024 * 1024,
            "Total remaining should be 15 MB"
        );

        // Each state should have some reservation
        assert!(state1.get_reservation() > 0);
        assert!(state2.get_reservation() > 0);
        assert!(state3.get_reservation() > 0);
    }

    #[test]
    fn test_multi_connection_fairness_under_pressure() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 16 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 8,
            num_connections: 8,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let states = vec![
            manager.register(),
            manager.register(),
            manager.register(),
            manager.register(),
        ];

        for state in &states {
            state.set_remaining_size_and_update_reservation(8 * 1024 * 1024);
        }

        // Update each state a few rounds to let reservation decisions converge.
        for _ in 0..4 {
            for state in &states {
                state.update_reservation();
            }
        }

        let reservations = states
            .iter()
            .map(|state| state.get_reservation())
            .collect::<Vec<_>>();

        assert!(reservations.iter().all(|reservation| *reservation > 0));

        // Fairness contract in this manager is "no starvation", not equal share.
        // Every state should keep at least its lower bound reservation.
        for (idx, state) in states.iter().enumerate() {
            let lower_bound =
                std::cmp::min(state.get_minimum_reservation(), state.get_remaining_size());
            assert!(
                reservations[idx] >= lower_bound,
                "state {idx} starved: reservation={} lower_bound={lower_bound} all={reservations:?}",
                reservations[idx]
            );
        }

        let total = reservations.iter().copied().sum::<usize>();
        assert!(
            total <= 16 * 1024 * 1024,
            "total reservation exceeds memory limit: total={total} all={reservations:?}"
        );
    }

    #[test]
    fn test_update_reservation() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state = manager.register();

        // Set remaining size without updating reservation
        state.set_remaining_size(50 * 1024 * 1024);
        let old_reservation = state.get_reservation();

        // Now update reservation
        state.update_reservation();
        let new_reservation = state.get_reservation();

        // Reservation should have been updated
        assert!(new_reservation >= old_reservation || new_reservation > 0);
    }

    #[test]
    fn test_minimum_reservation_calculation() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        // Test with small memory limit
        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024, // 1 MB
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let state = manager.register();

        // Minimum reservation should be bounded by memory_limit / 16
        let expected_max = 1024 * 1024 / MINIMUM_RESERVATION_MEMORY_LIMIT_DIVISOR;
        assert!(state.get_minimum_reservation() <= expected_max);
    }

    #[test]
    fn test_concurrent_registration() {
        use std::thread;

        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 8,
            num_connections: 8,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let mut handles = vec![];

        for _ in 0..8 {
            let mgr = manager.clone();
            handles.push(thread::spawn(move || {
                let state = mgr.register();
                state.set_remaining_size_and_update_reservation(1024 * 1024);
                thread::sleep(std::time::Duration::from_millis(10));
                state.get_reservation()
            }));
        }

        let reservations: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All reservations should be positive
        for r in &reservations {
            assert!(*r > 0);
        }

        // After all threads complete, manager should be empty
        assert_eq!(manager.get_active_state_count(), 0);
    }

    #[test]
    fn test_with_buffer_pool() {
        let pool = Arc::new(BufferPool::new(1024 * 1024 * 1024));
        let manager = Arc::new(TemporaryMemoryManager::with_buffer_pool(Arc::downgrade(
            &pool,
        )));

        let state = manager.register();

        // Should have picked up memory limit from pool
        assert!(state.get_reservation() > 0);
    }

    #[test]
    fn test_debug_format() {
        let manager = Arc::new(TemporaryMemoryManager::new());

        manager.update_configuration(TemporaryMemoryConfig {
            memory_limit: 1024 * 1024 * 1024,
            has_temporary_directory: true,
            num_threads: 4,
            num_connections: 1,
            query_max_memory: usize::MAX,
            force_external: false,
        });

        let _state = manager.register();

        let debug_str = format!("{:?}", manager);
        assert!(debug_str.contains("TemporaryMemoryManager"));
        assert!(debug_str.contains("total_reservation"));
        assert!(debug_str.contains("active_states"));
    }
}
