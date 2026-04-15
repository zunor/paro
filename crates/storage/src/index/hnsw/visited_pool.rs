//! # Visited Pool
//!
//! Thread-safe pool of visited-point trackers for HNSW search.
//!
//! ## Design Notes
//!
//! During HNSW search, we need to track which points have been visited.
//! Rather than allocating a new HashSet per search, we reuse VisitedList
//! instances from a pool. The VisitedList uses an iteration counter to
//! avoid O(n) resets — instead of clearing the vec, we just increment
//! the counter and compare against it.

use std::sync::RwLock;

use super::types::PointOffset;

/// Maximum number of VisitedList instances to keep in the pool.
const POOL_KEEP_LIMIT: usize = 16;

/// RAII handle for a VisitedList borrowed from a VisitedPool.
///
/// Automatically returns the list to the pool when dropped.
#[derive(Debug)]
pub struct VisitedListHandle<'a> {
    pool: &'a VisitedPool,
    visited_list: VisitedList,
}

/// Internal visited list that tracks which points have been seen.
///
/// Uses an iteration counter to avoid expensive full resets.
/// Each point has a counter value; it's "visited" if its counter
/// matches `current_iter`.
#[derive(Debug)]
struct VisitedList {
    current_iter: u8,
    visit_counters: Vec<u8>,
}

impl Default for VisitedList {
    fn default() -> Self {
        VisitedList {
            current_iter: 1,
            visit_counters: vec![],
        }
    }
}

impl VisitedList {
    fn new(num_points: usize) -> Self {
        VisitedList {
            current_iter: 1,
            visit_counters: vec![0; num_points],
        }
    }
}

impl Drop for VisitedListHandle<'_> {
    fn drop(&mut self) {
        self.pool
            .return_back(std::mem::take(&mut self.visited_list));
    }
}

impl<'a> VisitedListHandle<'a> {
    fn new(pool: &'a VisitedPool, data: VisitedList) -> Self {
        VisitedListHandle {
            pool,
            visited_list: data,
        }
    }

    /// Return `true` if the point was already visited in this iteration.
    pub fn check(&self, point_id: PointOffset) -> bool {
        self.visited_list
            .visit_counters
            .get(point_id as usize)
            .is_some_and(|x| *x == self.visited_list.current_iter)
    }

    /// Mark a point as visited. Returns `true` if it was already visited.
    pub fn check_and_update_visited(&mut self, point_id: PointOffset) -> bool {
        let idx = point_id as usize;
        if idx >= self.visited_list.visit_counters.len() {
            self.visited_list.visit_counters.resize(idx + 1, 0);
        }
        std::mem::replace(
            &mut self.visited_list.visit_counters[idx],
            self.visited_list.current_iter,
        ) == self.visited_list.current_iter
    }

    /// Advance to the next iteration, effectively "clearing" all visited markers.
    ///
    /// This is O(1) in the common case. On counter wraparound (every 255 iterations),
    /// it does an O(n) fill.
    pub fn next_iteration(&mut self) {
        self.visited_list.current_iter = self.visited_list.current_iter.wrapping_add(1);
        if self.visited_list.current_iter == 0 {
            self.visited_list.current_iter = 1;
            self.visited_list.visit_counters.fill(0);
        }
    }

    fn resize(&mut self, num_points: usize) {
        // `current_iter` is never 0, so 0 is safe as a default value.
        self.visited_list.visit_counters.resize(num_points, 0);
    }
}

/// Thread-safe pool of VisitedList instances.
///
/// Keeps a bounded number of lists for reuse, creating new ones
/// dynamically when the pool is empty.
#[derive(Debug)]
pub struct VisitedPool {
    pool: RwLock<Vec<VisitedList>>,
}

impl VisitedPool {
    pub fn new() -> Self {
        VisitedPool {
            pool: RwLock::new(Vec::with_capacity(POOL_KEEP_LIMIT)),
        }
    }

    /// Get a VisitedListHandle from the pool (or create a new one).
    ///
    /// The handle is automatically returned when dropped.
    pub fn get(&self, num_points: usize) -> VisitedListHandle<'_> {
        match self.pool.write().unwrap().pop() {
            None => VisitedListHandle::new(self, VisitedList::new(num_points)),
            Some(data) => {
                let mut visited_list = VisitedListHandle::new(self, data);
                visited_list.resize(num_points);
                visited_list.next_iteration();
                visited_list
            }
        }
    }

    fn return_back(&self, data: VisitedList) {
        let mut pool = self.pool.write().unwrap();
        if pool.len() < POOL_KEEP_LIMIT {
            pool.push(data);
        }
    }
}

impl Default for VisitedPool {
    fn default() -> Self {
        VisitedPool::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visited_list_basic() {
        let pool = VisitedPool::new();
        let mut visited = pool.get(10);

        // Initially not visited
        assert!(!visited.check(0));
        assert!(!visited.check(5));

        // Mark as visited
        assert!(!visited.check_and_update_visited(0)); // was not visited
        assert!(visited.check(0)); // now visited
        assert!(visited.check_and_update_visited(0)); // was already visited

        assert!(!visited.check_and_update_visited(5));
        assert!(visited.check(5));
    }

    #[test]
    fn test_visited_list_iteration_reset() {
        let pool = VisitedPool::new();
        let mut visited = pool.get(10);

        visited.check_and_update_visited(3);
        assert!(visited.check(3));

        // Next iteration clears all
        visited.next_iteration();
        assert!(!visited.check(3));
    }

    #[test]
    fn test_visited_list_counter_wraparound() {
        let pool = VisitedPool::new();
        let mut visited = pool.get(10);

        visited.check_and_update_visited(0);
        assert!(visited.check(0));

        // Force wraparound (255 + some extra iterations)
        for _ in 0..(u8::MAX as usize * 2 + 10) {
            visited.next_iteration();
            assert!(!visited.check(0));
        }
    }

    #[test]
    fn test_visited_list_auto_resize() {
        let pool = VisitedPool::new();
        let mut visited = pool.get(5);

        // Access beyond initial size
        assert!(!visited.check_and_update_visited(100));
        assert!(visited.check(100));
    }

    #[test]
    fn test_pool_reuse() {
        let pool = VisitedPool::new();

        // Get and return a list
        {
            let mut visited = pool.get(10);
            visited.check_and_update_visited(3);
        }
        // The list is returned to the pool

        // Get another — should reuse the returned list
        {
            let visited = pool.get(10);
            // After reuse + next_iteration, should be clean
            assert!(!visited.check(3));
        }
    }
}
