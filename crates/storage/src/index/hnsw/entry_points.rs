//! # Entry Points
//!
//! Manages entry points for HNSW search at different levels.

use super::types::PointOffset;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

const EXTRA_ENTRY_POINTS_LIMIT: usize = 10;

/// A single entry point in the HNSW graph.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntryPoint {
    /// Point index
    pub point_id: PointOffset,
    /// Level where this point is an entry point
    pub level: usize,
}

impl PartialOrd for EntryPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntryPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.level.cmp(&other.level)
    }
}

/// Manages entry points for HNSW search.
///
/// In a standard HNSW, there is one entry point at the highest level.
/// In Paro, we may keep multiple entry points to handle
/// filtered searches efficiently.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EntryPoints {
    /// The highest level entry points.
    /// Usually, we only have few of these.
    #[serde(default)]
    pub entry_points: Vec<EntryPoint>,
    /// Extra entry points (bounded, best-effort).
    #[serde(default)]
    pub extra_entry_points: Vec<EntryPoint>,
}

impl EntryPoints {
    pub fn new() -> Self {
        Self::default()
    }

    fn push_extra(&mut self, entry: EntryPoint) {
        if EXTRA_ENTRY_POINTS_LIMIT == 0 {
            return;
        }
        if self.extra_entry_points.len() < EXTRA_ENTRY_POINTS_LIMIT {
            self.extra_entry_points.push(entry);
            return;
        }
        let mut min_idx = 0;
        let mut min_level = self.extra_entry_points[0].level;
        for (idx, ep) in self.extra_entry_points.iter().enumerate().skip(1) {
            if ep.level < min_level {
                min_level = ep.level;
                min_idx = idx;
            }
        }
        if entry.level > min_level {
            self.extra_entry_points[min_idx] = entry;
        }
    }

    /// Add a new point as a potential entry point if it's on a high enough level.
    pub fn new_point<F>(&mut self, point_id: PointOffset, level: usize, checker: F)
    where
        F: Fn(PointOffset) -> bool,
    {
        // If there is an entry point for this filter group, return it.
        // Otherwise, register this as a new entry point.
        for i in 0..self.entry_points.len() {
            let candidate = self.entry_points[i];
            if !checker(candidate.point_id) {
                continue;
            }

            if candidate.level >= level {
                self.push_extra(EntryPoint { point_id, level });
                return;
            }

            // New point is better: replace and keep old as extra
            self.entry_points[i] = EntryPoint { point_id, level };
            self.push_extra(candidate);
            return;
        }

        self.entry_points.push(EntryPoint { point_id, level });
    }

    /// Get an entry point that satisfies the condition (e.g. not deleted and matches filter).
    pub fn get_entry_point<F>(&self, checker: F) -> Option<EntryPoint>
    where
        F: Fn(PointOffset) -> bool,
    {
        self.entry_points
            .iter()
            .find(|ep| checker(ep.point_id))
            .copied()
            .or_else(|| {
                self.extra_entry_points
                    .iter()
                    .filter(|ep| checker(ep.point_id))
                    .copied()
                    .max_by_key(|ep| ep.level)
            })
    }

    /// Get a random entry point that satisfies the condition.
    ///
    /// Uses reservoir sampling over both primary and extra entry points to
    /// avoid allocations and keep the pick uniform over all matches.
    pub fn get_random_entry_point<F, R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        checker: F,
    ) -> Option<EntryPoint>
    where
        F: Fn(PointOffset) -> bool,
    {
        let mut selected = None;
        let mut seen = 0usize;

        for entry in self
            .entry_points
            .iter()
            .chain(self.extra_entry_points.iter())
        {
            if !checker(entry.point_id) {
                continue;
            }

            seen += 1;
            if rng.gen_range(0..seen) == 0 {
                selected = Some(*entry);
            }
        }

        selected
    }

    /// Get the maximum level among all entry points.
    pub fn max_level(&self) -> usize {
        self.entry_points
            .iter()
            .map(|ep| ep.level)
            .max()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn test_entry_points_filtering_and_extras() {
        let mut points = EntryPoints::new();

        // Seed with a few groups via checker.
        points.new_point(1, 5, |_| true);
        points.new_point(2, 3, |_| true); // goes to extras
        points.new_point(3, 7, |_| true); // becomes main, old main -> extras

        assert_eq!(points.entry_points.len(), 1);
        assert_eq!(points.entry_points[0].point_id, 3);
        assert_eq!(points.entry_points[0].level, 7);
        assert!(!points.extra_entry_points.is_empty());

        // Create a second "group" that only matches even ids
        points.new_point(4, 6, |id| id % 2 == 0);
        points.new_point(6, 4, |id| id % 2 == 0);

        // We should now have two entry points (odd group and even group).
        assert_eq!(points.entry_points.len(), 2);

        // Query with checker for even ids should return the even entry point.
        let ep_even = points.get_entry_point(|id| id % 2 == 0).unwrap();
        assert_eq!(ep_even.point_id % 2, 0);

        // Query with checker for odd ids should return the odd entry point.
        let ep_odd = points.get_entry_point(|id| id % 2 == 1).unwrap();
        assert_eq!(ep_odd.point_id % 2, 1);
    }

    #[test]
    fn test_get_random_entry_point_uses_primary_and_extra_entries() {
        let points = EntryPoints {
            entry_points: vec![
                EntryPoint {
                    point_id: 10,
                    level: 8,
                },
                EntryPoint {
                    point_id: 20,
                    level: 7,
                },
            ],
            extra_entry_points: vec![
                EntryPoint {
                    point_id: 30,
                    level: 6,
                },
                EntryPoint {
                    point_id: 40,
                    level: 5,
                },
            ],
        };

        let mut rng = StdRng::seed_from_u64(42);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..200 {
            let ep = points
                .get_random_entry_point(&mut rng, |_| true)
                .expect("should pick an entry point");
            seen.insert(ep.point_id);
        }

        assert!(seen.contains(&10));
        assert!(seen.contains(&20));
        assert!(seen.contains(&30));
        assert!(seen.contains(&40));
    }

    #[test]
    fn test_get_random_entry_point_returns_none_when_no_match() {
        let mut points = EntryPoints::new();
        points.new_point(1, 5, |_| true);
        points.new_point(2, 4, |_| true);

        let mut rng = StdRng::seed_from_u64(7);
        assert!(points
            .get_random_entry_point(&mut rng, |id| id > 100)
            .is_none());
    }
}
