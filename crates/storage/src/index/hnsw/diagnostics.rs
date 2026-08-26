// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Explicit, governed structural diagnostics for immutable HNSW graphs.
//!
//! These scans are intentionally absent from artifact open and query hot
//! paths. They are used by offline build qualification and repair A/B work,
//! where O(N + E) evidence is preferable to inferring graph quality from
//! recall alone.

use paro_common::error::{self as paro_error, Result};
use serde::Serialize;

use super::entry_points::EntryPoints;
use super::graph_links::GraphLinks;
use super::types::PointOffset;
use crate::search::{ResourceBudget, SearchMemoryReservation};

const WORK_CHECK_POINT_INTERVAL: usize = 4096;

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct HnswDegreeSampleSummary {
    pub count: u64,
    pub min: u32,
    pub max: u32,
    pub mean: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct HnswTruthIndegreeComparison {
    pub found: HnswDegreeSampleSummary,
    pub missed: HnswDegreeSampleSummary,
}

/// Summary of level-0 reachability and degree shape.
///
/// `deterministic_entry_level0_reachable_*` follows level-0 outgoing edges from
/// the one entry selected by ordinary non-random search. The durable-entry
/// union is reported separately because it describes the ceiling of randomized
/// entry selection. These are layer-0 repair signals, not proofs of per-query
/// navigation: upper-layer descent can select another level-0 starting point.
/// `largest_weak_component_*` treats every edge as undirected; it is useful for
/// corruption/healing diagnostics but cannot prove navigability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct HnswGraphQualityReport {
    pub point_count: u64,
    pub level0_edge_count: u64,
    pub indegree_zero_count: u64,
    pub indegree_zero_ratio: f64,
    pub indegree_below_four_count: u64,
    pub indegree_below_four_ratio: f64,
    pub indegree_below_eight_count: u64,
    pub indegree_below_eight_ratio: f64,
    pub max_indegree: u32,
    pub mean_indegree: f64,
    pub deterministic_entry_level0_reachable_count: u64,
    pub deterministic_entry_level0_reachable_ratio: f64,
    pub durable_entry_union_level0_reachable_count: u64,
    pub durable_entry_union_level0_reachable_ratio: f64,
    pub largest_weak_component_count: u64,
    pub largest_weak_component_ratio: f64,
}

/// Governed diagnostic image retaining per-point indegrees for truth-neighbor
/// correlation. The logical memory reservation lives as long as the image.
#[derive(Debug)]
pub struct HnswGraphDiagnostics {
    report: HnswGraphQualityReport,
    indegrees: Box<[u32]>,
    _indegree_reservation: SearchMemoryReservation,
}

impl HnswGraphDiagnostics {
    /// Logical peak workspace required by [`Self::analyze`].
    ///
    /// The retained indegree image overlaps with either the directed
    /// reachability workspace or the weak-component disjoint set. The latter
    /// two phases execute serially and therefore share one admission grant.
    pub fn estimated_peak_memory_bytes(point_count: usize) -> Result<usize> {
        let (indegree_bytes, workspace_bytes) = diagnostic_memory_shape(point_count)?;
        indegree_bytes
            .checked_add(workspace_bytes)
            .ok_or_else(|| paro_error::out_of_range("HNSW diagnostic peak memory overflow"))
    }

    pub fn analyze(
        links: &GraphLinks,
        entry_points: &EntryPoints,
        budget: &ResourceBudget,
    ) -> Result<Self> {
        let point_count = links.num_points();
        let (indegree_bytes, workspace_bytes) = diagnostic_memory_shape(point_count)?;
        let indegree_reservation = budget.try_reserve_memory(indegree_bytes)?;
        // Admit the complete peak before the first O(E) pass. Otherwise a
        // caller with an undersized budget would pay the indegree scan only to
        // fail when the later reachability/component workspace is allocated.
        let _workspace_reservation = budget.try_reserve_memory(workspace_bytes)?;
        let mut indegrees = zeroed_vec::<u32>(point_count, "HNSW indegree vector")?;
        let mut edge_count = 0u64;
        let mut work = WorkCheckpoint::default();

        for point in point_domain(point_count)? {
            let mut degree = 0usize;
            let mut indegree_overflow = false;
            let mut invalid_neighbor = None;
            links.for_each_link(point, 0, |neighbor| {
                if neighbor as usize >= point_count {
                    invalid_neighbor = Some(neighbor);
                    return;
                }
                let slot = &mut indegrees[neighbor as usize];
                if let Some(next) = slot.checked_add(1) {
                    *slot = next;
                } else {
                    indegree_overflow = true;
                }
                degree += 1;
            })?;
            if let Some(neighbor) = invalid_neighbor {
                return Err(invalid_neighbor_error(point, neighbor, point_count));
            }
            if indegree_overflow {
                return Err(paro_error::data_corrupted(
                    "HNSW level-0 indegree exceeds the u32 point domain",
                ));
            }
            edge_count = edge_count
                .checked_add(degree as u64)
                .ok_or_else(|| paro_error::out_of_range("HNSW level-0 edge count overflow"))?;
            work.observe(degree, budget)?;
        }
        work.finish(budget)?;

        let indegree_zero_count = indegrees.iter().filter(|&&degree| degree == 0).count() as u64;
        let indegree_below_four_count =
            indegrees.iter().filter(|&&degree| degree < 4).count() as u64;
        let indegree_below_eight_count =
            indegrees.iter().filter(|&&degree| degree < 8).count() as u64;
        let max_indegree = indegrees.iter().copied().max().unwrap_or(0);
        let mean_indegree = ratio(edge_count, point_count as u64);
        let deterministic_entry_level0_reachable_count = directed_reachable_count(
            links,
            entry_points
                .get_entry_point(|_| true)
                .into_iter()
                .map(|entry| entry.point_id),
            budget,
        )?;
        let durable_entry_union_level0_reachable_count =
            if deterministic_entry_level0_reachable_count == point_count as u64 {
                deterministic_entry_level0_reachable_count
            } else {
                directed_reachable_count(
                    links,
                    entry_points
                        .entry_points
                        .iter()
                        .chain(entry_points.extra_entry_points.iter())
                        .map(|entry| entry.point_id),
                    budget,
                )?
            };
        let largest_weak_component_count = largest_weak_component_count(links, budget)?;

        Ok(Self {
            report: HnswGraphQualityReport {
                point_count: point_count as u64,
                level0_edge_count: edge_count,
                indegree_zero_count,
                indegree_zero_ratio: ratio(indegree_zero_count, point_count as u64),
                indegree_below_four_count,
                indegree_below_four_ratio: ratio(indegree_below_four_count, point_count as u64),
                indegree_below_eight_count,
                indegree_below_eight_ratio: ratio(indegree_below_eight_count, point_count as u64),
                max_indegree,
                mean_indegree,
                deterministic_entry_level0_reachable_count,
                deterministic_entry_level0_reachable_ratio: ratio(
                    deterministic_entry_level0_reachable_count,
                    point_count as u64,
                ),
                durable_entry_union_level0_reachable_count,
                durable_entry_union_level0_reachable_ratio: ratio(
                    durable_entry_union_level0_reachable_count,
                    point_count as u64,
                ),
                largest_weak_component_count,
                largest_weak_component_ratio: ratio(
                    largest_weak_component_count,
                    point_count as u64,
                ),
            },
            indegrees: indegrees.into_boxed_slice(),
            _indegree_reservation: indegree_reservation,
        })
    }

    pub const fn report(&self) -> HnswGraphQualityReport {
        self.report
    }

    pub fn indegree(&self, point: PointOffset) -> Result<u32> {
        self.indegrees.get(point as usize).copied().ok_or_else(|| {
            paro_error::out_of_range(format!(
                "HNSW diagnostic point {point} exceeds {}-point domain",
                self.indegrees.len()
            ))
        })
    }

    /// Level-0 indegree image in artifact-local point-id order.
    ///
    /// Offline qualification tools may persist this compact image next to
    /// benchmark truth so found/missed correlation does not rescan the graph.
    /// Query execution must not retain or consult it.
    pub fn indegrees(&self) -> &[u32] {
        &self.indegrees
    }

    /// Compare level-0 indegrees for exact truth neighbors found and missed by
    /// one approximate query. This is the go/no-go signal for inbound-edge
    /// repair: a systematic missed-point deficit supports a reachability
    /// hypothesis; equal distributions point toward navigation quality.
    pub fn compare_truth_indegrees(
        &self,
        actual: &[PointOffset],
        truth: &[PointOffset],
    ) -> Result<HnswTruthIndegreeComparison> {
        let mut found = DegreeSamples::default();
        let mut missed = DegreeSamples::default();
        for &point in truth {
            let degree = self.indegree(point)?;
            if actual.contains(&point) {
                found.push(degree);
            } else {
                missed.push(degree);
            }
        }
        Ok(HnswTruthIndegreeComparison {
            found: found.summary(),
            missed: missed.summary(),
        })
    }
}

#[derive(Default)]
struct DegreeSamples {
    count: u64,
    sum: u64,
    min: u32,
    max: u32,
}

impl DegreeSamples {
    fn push(&mut self, degree: u32) {
        if self.count == 0 {
            self.min = degree;
        } else {
            self.min = self.min.min(degree);
        }
        self.max = self.max.max(degree);
        self.sum = self.sum.saturating_add(u64::from(degree));
        self.count = self.count.saturating_add(1);
    }

    fn summary(self) -> HnswDegreeSampleSummary {
        HnswDegreeSampleSummary {
            count: self.count,
            min: self.min,
            max: self.max,
            mean: if self.count == 0 {
                0.0
            } else {
                self.sum as f64 / self.count as f64
            },
        }
    }
}

#[derive(Default)]
struct WorkCheckpoint {
    points: usize,
    edges: usize,
}

impl WorkCheckpoint {
    fn observe(&mut self, edges: usize, budget: &ResourceBudget) -> Result<()> {
        self.points = self.points.saturating_add(1);
        self.edges = self.edges.saturating_add(edges);
        if self.points >= WORK_CHECK_POINT_INTERVAL {
            self.flush(budget)?;
        }
        Ok(())
    }

    fn finish(&mut self, budget: &ResourceBudget) -> Result<()> {
        if self.points != 0 || self.edges != 0 {
            self.flush(budget)?;
        }
        Ok(())
    }

    fn flush(&mut self, budget: &ResourceBudget) -> Result<()> {
        let steps = self.points.saturating_add(self.edges);
        self.points = 0;
        self.edges = 0;
        budget.work.check_and_consume(steps)
    }
}

fn directed_reachable_count(
    links: &GraphLinks,
    entries: impl IntoIterator<Item = PointOffset>,
    budget: &ResourceBudget,
) -> Result<u64> {
    let point_count = links.num_points();
    if point_count == 0 {
        return Ok(0);
    }
    let word_count = bitmap_word_count(point_count);
    let mut visited = zeroed_vec::<u64>(word_count, "HNSW reachability bitmap")?;
    let mut stack = Vec::<PointOffset>::new();
    stack
        .try_reserve_exact(point_count)
        .map_err(|_| paro_error::out_of_memory("allocate HNSW reachability stack"))?;

    for entry in entries {
        if entry as usize >= point_count {
            return Err(paro_error::data_corrupted(format!(
                "HNSW entry point {} exceeds {}-point graph",
                entry, point_count
            )));
        }
        if mark_visited(&mut visited, entry) {
            stack.push(entry);
        }
    }
    if stack.is_empty() {
        return Err(paro_error::data_corrupted(
            "non-empty HNSW graph has no durable entry point",
        ));
    }

    let mut reachable = stack.len() as u64;
    let mut work = WorkCheckpoint::default();
    while let Some(point) = stack.pop() {
        let mut degree = 0usize;
        let mut invalid_neighbor = None;
        links.for_each_link(point, 0, |neighbor| {
            degree += 1;
            if neighbor as usize >= point_count {
                invalid_neighbor = Some(neighbor);
                return;
            }
            if mark_visited(&mut visited, neighbor) {
                // `mark_visited` admits each point once and the graph domain is
                // bounded by `PointOffset`, so this sum cannot overflow u64.
                reachable += 1;
                stack.push(neighbor);
            }
        })?;
        if let Some(neighbor) = invalid_neighbor {
            return Err(invalid_neighbor_error(point, neighbor, point_count));
        }
        work.observe(degree, budget)?;
    }
    work.finish(budget)?;
    Ok(reachable)
}

fn largest_weak_component_count(links: &GraphLinks, budget: &ResourceBudget) -> Result<u64> {
    let point_count = links.num_points();
    if point_count == 0 {
        return Ok(0);
    }
    let mut components = Vec::<i64>::new();
    components
        .try_reserve_exact(point_count)
        .map_err(|_| paro_error::out_of_memory("allocate HNSW weak-component disjoint set"))?;
    components.resize(point_count, -1);

    let mut work = WorkCheckpoint::default();
    for point in point_domain(point_count)? {
        let mut degree = 0usize;
        let mut invalid_neighbor = None;
        links.for_each_link(point, 0, |neighbor| {
            degree += 1;
            if neighbor as usize >= point_count {
                invalid_neighbor = Some(neighbor);
                return;
            }
            union_components(&mut components, point as usize, neighbor as usize);
        })?;
        if let Some(neighbor) = invalid_neighbor {
            return Err(invalid_neighbor_error(point, neighbor, point_count));
        }
        work.observe(degree, budget)?;
    }
    work.finish(budget)?;

    Ok(components
        .iter()
        .filter(|&&entry| entry < 0)
        .map(|&entry| entry.unsigned_abs())
        .max()
        .unwrap_or(0))
}

fn invalid_neighbor_error(
    point: PointOffset,
    neighbor: PointOffset,
    point_count: usize,
) -> paro_common::error::ParoError {
    paro_error::data_corrupted(format!(
        "HNSW level-0 edge {point}->{neighbor} exceeds {point_count}-point graph"
    ))
}

fn find_component(components: &mut [i64], mut point: usize) -> usize {
    let mut root = point;
    while components[root] >= 0 {
        root = components[root] as usize;
    }
    while point != root {
        let parent = components[point] as usize;
        components[point] = root as i64;
        point = parent;
    }
    root
}

fn union_components(components: &mut [i64], left: usize, right: usize) {
    let mut left_root = find_component(components, left);
    let mut right_root = find_component(components, right);
    if left_root == right_root {
        return;
    }
    // More-negative means larger. The point-id tie break makes the diagnostic
    // image reproducible even though only the final component size is exposed.
    if components[left_root] > components[right_root]
        || (components[left_root] == components[right_root] && left_root > right_root)
    {
        std::mem::swap(&mut left_root, &mut right_root);
    }
    components[left_root] = components[left_root].saturating_add(components[right_root]);
    components[right_root] = left_root as i64;
}

fn mark_visited(words: &mut [u64], point: PointOffset) -> bool {
    let point = point as usize;
    let word = point / u64::BITS as usize;
    let mask = 1u64 << (point % u64::BITS as usize);
    let unseen = words[word] & mask == 0;
    words[word] |= mask;
    unseen
}

fn point_domain(point_count: usize) -> Result<std::ops::Range<PointOffset>> {
    let end = PointOffset::try_from(point_count)
        .map_err(|_| paro_error::out_of_range("HNSW graph exceeds u32 point-id domain"))?;
    Ok(0..end)
}

fn diagnostic_memory_shape(point_count: usize) -> Result<(usize, usize)> {
    let indegree_bytes = allocation_bytes::<u32>(point_count, "HNSW indegree vector")?;
    let reachability_bytes =
        allocation_bytes::<u64>(bitmap_word_count(point_count), "HNSW reachability bitmap")?
            .checked_add(allocation_bytes::<PointOffset>(
                point_count,
                "HNSW reachability stack",
            )?)
            .ok_or_else(|| paro_error::out_of_range("HNSW reachability workspace size overflow"))?;
    let component_bytes = allocation_bytes::<i64>(point_count, "HNSW weak-component disjoint set")?;
    Ok((indegree_bytes, reachability_bytes.max(component_bytes)))
}

fn bitmap_word_count(point_count: usize) -> usize {
    point_count.div_ceil(u64::BITS as usize)
}

fn allocation_bytes<T>(len: usize, label: &str) -> Result<usize> {
    len.checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| paro_error::out_of_range(format!("{label} size overflow")))
}

fn zeroed_vec<T: Default + Clone>(len: usize, label: &str) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| paro_error::out_of_memory(format!("allocate {label}")))?;
    values.resize(len, T::default());
    Ok(values)
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        1.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::{EntryPoint, GraphLinks};
    use crate::search::budget::NoopSearchCancellation;
    use std::sync::Arc;

    fn disconnected_graph() -> (GraphLinks, EntryPoints) {
        let links = GraphLinks::new_from_edges(vec![
            vec![vec![1]],
            vec![vec![0, 2]],
            vec![vec![1]],
            vec![vec![4]],
            vec![vec![3]],
        ]);
        let mut entries = EntryPoints::new();
        entries.new_point(0, 0, |_| true);
        entries.extra_entry_points.push(EntryPoint {
            point_id: 3,
            level: 0,
        });
        (links, entries)
    }

    #[test]
    fn diagnostics_distinguish_directed_reachability_from_weak_connectivity() {
        // The default entry 0 reaches {0,1,2}; an extra durable entry reaches
        // {3,4}. Their union covers the graph, while no weak component does.
        let (links, entries) = disconnected_graph();
        let budget = ResourceBudget::standalone(1 << 20, 1024, 1);

        let diagnostics = HnswGraphDiagnostics::analyze(&links, &entries, &budget).unwrap();
        let report = diagnostics.report();
        assert_eq!(report.point_count, 5);
        assert_eq!(report.level0_edge_count, 6);
        assert_eq!(report.indegree_zero_count, 0);
        assert_eq!(report.indegree_below_four_count, 5);
        assert_eq!(report.indegree_below_eight_count, 5);
        assert_eq!(report.indegree_zero_ratio, 0.0);
        assert_eq!(report.indegree_below_four_ratio, 1.0);
        assert_eq!(report.indegree_below_eight_ratio, 1.0);
        assert_eq!(report.deterministic_entry_level0_reachable_count, 3);
        assert_eq!(report.deterministic_entry_level0_reachable_ratio, 0.6);
        assert_eq!(report.durable_entry_union_level0_reachable_count, 5);
        assert_eq!(report.durable_entry_union_level0_reachable_ratio, 1.0);
        assert_eq!(report.largest_weak_component_count, 3);
        assert_eq!(report.largest_weak_component_ratio, 0.6);

        let comparison = diagnostics
            .compare_truth_indegrees(&[0], &[0, 1, 2])
            .unwrap();
        assert_eq!(comparison.found.count, 1);
        assert_eq!(comparison.missed.count, 2);
        assert_eq!(comparison.found.mean, 1.0);
        assert_eq!(comparison.missed.mean, 1.5);
    }

    #[test]
    fn diagnostics_obey_memory_and_work_governance() {
        let (links, entries) = disconnected_graph();
        assert_eq!(
            HnswGraphDiagnostics::estimated_peak_memory_bytes(5).unwrap(),
            60
        );
        let memory_limited = ResourceBudget::standalone(1, 1024, 1);
        assert!(
            HnswGraphDiagnostics::analyze(&links, &entries, &memory_limited)
                .unwrap_err()
                .to_string()
                .contains("query budget")
        );

        let work_limited = ResourceBudget::standalone(1 << 20, 1024, 1)
            .with_work_controls(Some(1), Arc::new(NoopSearchCancellation));
        assert!(
            HnswGraphDiagnostics::analyze(&links, &entries, &work_limited)
                .unwrap_err()
                .to_string()
                .contains("CPU step budget")
        );
    }
}
