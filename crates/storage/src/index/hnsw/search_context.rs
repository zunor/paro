// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! # Search Context
//!
//! Manages search state (nearest neighbors and candidates) during HNSW lookup.

use super::types::{ScoreType, ScoredPoint};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// A priority queue with a fixed maximum length that keeps the "best" elements.
///
/// Under the hood, it's a Min-Heap of size K. The smallest element is at the top.
/// If a new element is better than the smallest, it replaces it.
#[derive(Debug, Clone)]
pub struct FixedLengthPriorityQueue<T: Ord> {
    pub heap: BinaryHeap<Reverse<T>>,
    pub capacity: usize,
}

/// Bounded Top-K collector for long sequential scans with a small result K.
///
/// A binary heap pays guard/sift machinery for every rejected input. Exact
/// scans have the opposite workload: after the first K rows, almost every row
/// loses one comparison against the current floor and only O(K log N) rows
/// replace it. Keeping the K values unsorted makes that common path one
/// comparison; the collector rescans K only after a successful replacement.
/// Graph beams deliberately keep [`FixedLengthPriorityQueue`], where K is much
/// wider and competitive insertions are frequent.
#[derive(Debug, Clone)]
pub(crate) struct ScanTopK<T: Ord> {
    values: Vec<T>,
    capacity: usize,
    worst_index: Option<usize>,
}

impl<T: Ord> ScanTopK<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            capacity,
            worst_index: None,
        }
    }

    #[inline(always)]
    pub(crate) fn push(&mut self, element: T) -> bool {
        if self.capacity == 0 {
            return false;
        }
        if self.values.len() < self.capacity {
            self.values.push(element);
            if self.values.len() == self.capacity {
                self.recompute_worst();
            }
            return true;
        }

        let worst_index = self
            .worst_index
            .expect("a full scan Top-K collector has a floor");
        if element <= self.values[worst_index] {
            return false;
        }
        self.values[worst_index] = element;
        self.recompute_worst();
        true
    }

    #[inline(always)]
    fn recompute_worst(&mut self) {
        debug_assert!(!self.values.is_empty());
        let mut worst = 0usize;
        for index in 1..self.values.len() {
            if self.values[index] < self.values[worst] {
                worst = index;
            }
        }
        self.worst_index = Some(worst);
    }

    pub(crate) fn into_sorted_vec(mut self) -> Vec<T> {
        self.values.sort_unstable_by(|a, b| b.cmp(a));
        self.values
    }
}

impl<T: Ord> FixedLengthPriorityQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(capacity),
            capacity,
        }
    }

    /// Retain `element` when it belongs to the bounded best set.
    ///
    /// Returning the admission decision lets graph traversal publish a point
    /// to its expansion frontier without first peeking and comparing the same
    /// heap a second time.
    #[inline(always)]
    pub fn push(&mut self, element: T) -> bool {
        if self.heap.len() < self.capacity {
            self.heap.push(Reverse(element));
            true
        } else if let Some(mut worst) = self.heap.peek_mut() {
            if element > worst.0 {
                *worst = Reverse(element);
                return true;
            }
            false
        } else {
            false
        }
    }

    /// Convert into a sorted vector (best results first).
    pub fn into_sorted_vec(self) -> Vec<T> {
        let mut vec = self.into_unsorted_vec();
        // Heap entries have a total order. Graph traversal de-duplicates point
        // ids before insertion, so stable ordering has no semantic value and
        // only adds an allocation-backed merge-sort path to every build point.
        vec.sort_unstable_by(|a, b| b.cmp(a)); // Descending order (best first)
        vec
    }

    /// Materialize only the requested best prefix in result order.
    ///
    /// HNSW navigation retains `ef` points to control recall, while SQL usually
    /// asks for a much smaller K. Sorting the complete beam makes result
    /// materialization O(ef log ef) even though only K rows escape the graph.
    /// An order statistic keeps this boundary O(ef + K log K).
    pub fn into_top_sorted_vec(self, limit: usize) -> Vec<T> {
        if limit == 0 {
            return Vec::new();
        }
        let mut vec = self.into_unsorted_vec();
        if vec.len() > limit {
            vec.select_nth_unstable_by(limit, |a, b| b.cmp(a));
            vec.truncate(limit);
        }
        vec.sort_unstable_by(|a, b| b.cmp(a));
        vec
    }

    /// Consume the heap without imposing result order. Strategy decisions can
    /// inspect admission counts and order statistics without paying for a
    /// complete sort that may be discarded by an adaptive retry.
    pub fn into_unsorted_vec(self) -> Vec<T> {
        self.heap.into_iter().map(|Reverse(x)| x).collect()
    }

    /// Get the "worst" (minimum) element in the queue.
    #[inline(always)]
    pub fn min_element(&self) -> Option<&T> {
        self.heap.peek().map(|Reverse(x)| x)
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Whether the queue has observed enough competitive elements to fill its
    /// retained window. Adaptive callers must use this signal instead of
    /// comparing a requested headroom larger than the queue's capacity.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.heap.len() == self.capacity
    }
}

/// Search context for HNSW search.
///
/// Tracks the best points found so far (nearest) and the points to explore (candidates).
#[derive(Debug)]
pub struct SearchContext {
    /// Best results found so far (Min-Heap of size `ef`, keeps the `ef` largest scores)
    pub nearest: FixedLengthPriorityQueue<ScoredPoint>,
    /// Candidates to explore (Max-Heap, priority by score)
    pub candidates: BinaryHeap<ScoredPoint>,
}

impl SearchContext {
    pub fn new(entry_point: ScoredPoint, ef: usize) -> Self {
        let mut nearest = FixedLengthPriorityQueue::new(ef);
        nearest.push(entry_point);

        let mut candidates = BinaryHeap::with_capacity(ef);
        candidates.push(entry_point);

        Self {
            nearest,
            candidates,
        }
    }

    /// Process a new candidate point.
    ///
    /// If the point is good enough to enter the `nearest` set, it's also added to `candidates`.
    #[inline(always)]
    pub fn process_candidate(&mut self, point: ScoredPoint) -> bool {
        if self.nearest.push(point) {
            self.candidates.push(point);
            true
        } else {
            false
        }
    }

    /// Current lower bound score (worst score in the `nearest` set).
    #[inline(always)]
    pub fn lower_bound(&self) -> ScoreType {
        self.nearest
            .min_element()
            .map(|p| p.score)
            .unwrap_or(f32::MIN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_length_queue() {
        let mut q = FixedLengthPriorityQueue::new(3);
        assert!(q.push(ScoredPoint { idx: 1, score: 0.5 }));
        assert!(q.push(ScoredPoint { idx: 2, score: 0.8 }));
        assert!(q.push(ScoredPoint { idx: 3, score: 0.3 }));
        assert!(q.push(ScoredPoint { idx: 4, score: 0.9 }));
        assert!(!q.push(ScoredPoint { idx: 5, score: 0.1 }));

        let results = q.into_sorted_vec();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].score, 0.9);
        assert_eq!(results[1].score, 0.8);
        assert_eq!(results[2].score, 0.5);
    }

    #[test]
    fn scan_top_k_matches_heap_collector_across_replacements() {
        let values = [
            0.1, 0.8, 0.3, 0.9, 0.5, 0.7, 0.2, 0.6, 1.0, -0.4, 0.81, 0.79,
        ];
        let mut heap = FixedLengthPriorityQueue::new(4);
        let mut scan = ScanTopK::new(4);
        for (idx, score) in values.into_iter().enumerate() {
            let point = ScoredPoint {
                idx: idx as u32,
                score,
            };
            assert_eq!(scan.push(point), heap.push(point));
        }
        assert_eq!(scan.into_sorted_vec(), heap.into_sorted_vec());
    }

    #[test]
    fn scan_top_k_zero_capacity_rejects_everything() {
        let mut scan = ScanTopK::new(0);
        assert!(!scan.push(ScoredPoint { idx: 0, score: 1.0 }));
        assert!(scan.into_sorted_vec().is_empty());
    }

    #[test]
    fn fixed_length_queue_materializes_only_best_prefix() {
        let mut q = FixedLengthPriorityQueue::new(8);
        for (idx, score) in [0.1, 0.8, 0.3, 0.9, 0.5, 0.7, 0.2, 0.6]
            .into_iter()
            .enumerate()
        {
            q.push(ScoredPoint {
                idx: idx as u32,
                score,
            });
        }

        let results = q.into_top_sorted_vec(3);
        assert_eq!(
            results.iter().map(|point| point.score).collect::<Vec<_>>(),
            vec![0.9, 0.8, 0.7]
        );
    }

    #[test]
    fn test_search_context() {
        let entry = ScoredPoint { idx: 0, score: 0.5 };
        let mut ctx = SearchContext::new(entry, 2);

        assert_eq!(ctx.lower_bound(), 0.5);

        // Better point
        assert!(ctx.process_candidate(ScoredPoint { idx: 1, score: 0.8 }));
        assert_eq!(ctx.lower_bound(), 0.5); // Still 0.5 because nearest size is 2

        // Even better point — replaces 0.5
        assert!(ctx.process_candidate(ScoredPoint { idx: 2, score: 0.9 }));
        assert_eq!(ctx.lower_bound(), 0.8);

        // Worse point - ignored
        assert!(!ctx.process_candidate(ScoredPoint { idx: 3, score: 0.4 }));
    }
}
