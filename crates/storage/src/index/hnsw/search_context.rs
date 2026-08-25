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
        let mut vec: Vec<_> = self.heap.into_iter().map(|Reverse(x)| x).collect();
        // Heap entries have a total order. Graph traversal de-duplicates point
        // ids before insertion, so stable ordering has no semantic value and
        // only adds an allocation-backed merge-sort path to every build point.
        vec.sort_unstable_by(|a, b| b.cmp(a)); // Descending order (best first)
        vec
    }

    /// Get the "worst" (minimum) element in the queue.
    pub fn min_element(&self) -> Option<&T> {
        self.heap.peek().map(|Reverse(x)| x)
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Whether the queue has observed enough competitive elements to fill its
    /// retained window. Adaptive callers must use this signal instead of
    /// comparing a requested headroom larger than the queue's capacity.
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
    pub fn process_candidate(&mut self, point: ScoredPoint) -> bool {
        if self.nearest.push(point) {
            self.candidates.push(point);
            true
        } else {
            false
        }
    }

    /// Current lower bound score (worst score in the `nearest` set).
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
