//! Delta adjacency buffer for incremental graph index updates.
//!
//! After `CREATE PROPERTY GRAPH` builds the initial CSR, subsequent
//! INSERT / DELETE operations on the underlying edge tables are captured
//! in a `DeltaAdjacency` buffer.  At query time the executor merges
//! CSR results with the delta:
//!
//!   effective_neighbors(v) = CSR.neighbors(v) + added − deleted
//!
//! When the delta grows large enough a *compaction* pass rebuilds the
//! CSR from scratch, absorbing all pending deltas.

use std::collections::{HashMap, HashSet};

use paro_common::error::{self as paro_error, Result};

use super::adjacency_csr::AdjacencyCSR;
use super::vertex_id_map::{LocalVertexId, VertexIdMap, VertexKey};

/// A single added edge in the delta buffer.
#[derive(Debug, Clone)]
pub struct DeltaEdge {
    /// Source vertex local id.
    pub src: LocalVertexId,
    /// Destination vertex local id.
    pub dst: LocalVertexId,
    /// Row id of the edge in the underlying edge table.
    pub edge_rowid: u64,
}

/// Incremental adjacency buffer that overlays a base `AdjacencyCSR`.
///
/// Thread-safety: the caller (typically `GraphProjectionIndex`) is
/// responsible for synchronisation.  `DeltaAdjacency` itself is `Send`.
#[derive(Debug)]
pub struct DeltaAdjacency {
    /// Edges added since the last CSR build / compaction.
    added_edges: Vec<DeltaEdge>,
    /// Forward adjacency list for added edges: src → [(dst, edge_rowid)].
    added_forward: HashMap<LocalVertexId, Vec<(LocalVertexId, u64)>>,
    /// Backward adjacency list for added edges: dst → [(src, edge_rowid)].
    added_backward: HashMap<LocalVertexId, Vec<(LocalVertexId, u64)>>,
    /// Row ids of edges that have been deleted.
    deleted_edge_rowids: HashSet<u64>,
}

impl DeltaAdjacency {
    /// Create an empty delta buffer.
    pub fn new() -> Self {
        Self {
            added_edges: Vec::new(),
            added_forward: HashMap::new(),
            added_backward: HashMap::new(),
            deleted_edge_rowids: HashSet::new(),
        }
    }

    // ── Mutation ──────────────────────────────────────────────

    /// Record a newly inserted edge.
    ///
    /// `src_key` / `dst_key` are the vertex primary keys as stored in the
    /// edge table.  They are resolved to local vertex ids via the provided
    /// vertex maps.  If either key is unknown the call returns an error.
    pub fn add_edge(
        &mut self,
        src_key: &VertexKey,
        dst_key: &VertexKey,
        edge_rowid: u64,
        src_vertex_map: &VertexIdMap,
        dst_vertex_map: &VertexIdMap,
    ) -> Result<()> {
        let src = src_vertex_map.key_to_local(src_key).ok_or_else(|| {
            paro_error::invalid_input(format!(
                "DeltaAdjacency::add_edge: source key {:?} not found in vertex map",
                src_key
            ))
        })?;
        let dst = dst_vertex_map.key_to_local(dst_key).ok_or_else(|| {
            paro_error::invalid_input(format!(
                "DeltaAdjacency::add_edge: destination key {:?} not found in vertex map",
                dst_key
            ))
        })?;
        self.add_edge_by_local_id(src, dst, edge_rowid);
        Ok(())
    }

    /// Record a newly inserted edge using pre-resolved local vertex ids.
    pub fn add_edge_by_local_id(
        &mut self,
        src: LocalVertexId,
        dst: LocalVertexId,
        edge_rowid: u64,
    ) {
        self.added_edges.push(DeltaEdge {
            src,
            dst,
            edge_rowid,
        });
        self.added_forward
            .entry(src)
            .or_default()
            .push((dst, edge_rowid));
        self.added_backward
            .entry(dst)
            .or_default()
            .push((src, edge_rowid));
    }

    /// Mark an edge as deleted by its row id.
    pub fn delete_edge(&mut self, edge_rowid: u64) {
        self.deleted_edge_rowids.insert(edge_rowid);
    }

    /// Check whether an edge rowid has been marked as deleted.
    #[inline]
    pub fn is_deleted(&self, edge_rowid: u64) -> bool {
        self.deleted_edge_rowids.contains(&edge_rowid)
    }

    // ── Query ────────────────────────────────────────────────

    /// Return the effective forward neighbors of `v`, merging the base
    /// CSR with the delta buffer.
    ///
    /// Returns `(neighbor_local_id, edge_rowid)` pairs.
    pub fn neighbors_merged_forward(
        &self,
        v: LocalVertexId,
        base_csr: &AdjacencyCSR,
    ) -> Vec<(LocalVertexId, u64)> {
        let mut result = Vec::new();
        self.fill_neighbors_merged_forward(v, base_csr, &mut result);
        result
    }

    pub fn fill_neighbors_merged_forward(
        &self,
        v: LocalVertexId,
        base_csr: &AdjacencyCSR,
        result: &mut Vec<(LocalVertexId, u64)>,
    ) {
        result.clear();

        // 1. Base CSR neighbors, minus deleted.
        let nbrs = base_csr.neighbors(v);
        let eids = base_csr.edge_rowids_for(v);
        for (i, &dst) in nbrs.iter().enumerate() {
            let eid = eids[i];
            if !self.is_deleted(eid) {
                result.push((dst, eid));
            }
        }

        // 2. Added forward edges from v.
        if let Some(added) = self.added_forward.get(&v) {
            for &(dst, eid) in added {
                if !self.is_deleted(eid) {
                    result.push((dst, eid));
                }
            }
        }
    }

    /// Return the effective backward neighbors of `v`, merging the base
    /// CSR with the delta buffer.
    pub fn neighbors_merged_backward(
        &self,
        v: LocalVertexId,
        base_csr: &AdjacencyCSR,
    ) -> Vec<(LocalVertexId, u64)> {
        let mut result = Vec::new();
        self.fill_neighbors_merged_backward(v, base_csr, &mut result);
        result
    }

    pub fn fill_neighbors_merged_backward(
        &self,
        v: LocalVertexId,
        base_csr: &AdjacencyCSR,
        result: &mut Vec<(LocalVertexId, u64)>,
    ) {
        result.clear();
        let nbrs = base_csr.neighbors(v);
        let eids = base_csr.edge_rowids_for(v);
        for (i, &dst) in nbrs.iter().enumerate() {
            let eid = eids[i];
            if !self.is_deleted(eid) {
                result.push((dst, eid));
            }
        }

        if let Some(added) = self.added_backward.get(&v) {
            for &(src, eid) in added {
                if !self.is_deleted(eid) {
                    result.push((src, eid));
                }
            }
        }
    }

    // ── Statistics ───────────────────────────────────────────

    /// Number of edges in the added buffer.
    pub fn added_count(&self) -> usize {
        self.added_edges.len()
    }

    /// Number of edges marked as deleted.
    pub fn deleted_count(&self) -> u64 {
        self.deleted_edge_rowids.len() as u64
    }

    /// Whether the delta is empty (no adds, no deletes).
    pub fn is_empty(&self) -> bool {
        self.added_edges.is_empty() && self.deleted_edge_rowids.is_empty()
    }

    /// Approximate heap memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        let edges = self.added_edges.capacity() * std::mem::size_of::<DeltaEdge>();
        let fwd: usize = self
            .added_forward
            .values()
            .map(|v| v.capacity() * std::mem::size_of::<(LocalVertexId, u64)>())
            .sum();
        let bwd: usize = self
            .added_backward
            .values()
            .map(|v| v.capacity() * std::mem::size_of::<(LocalVertexId, u64)>())
            .sum();
        let deleted = self.deleted_edge_rowids.capacity() * std::mem::size_of::<u64>();
        edges + fwd + bwd + deleted
    }

    // ── Compaction ───────────────────────────────────────────

    /// Produce a new set of CSR edges that merges the base CSR with this
    /// delta.  The caller can feed the result into `AdjacencyCSR::build`
    /// to create a compacted CSR.
    ///
    /// Returns `(src, dst, edge_rowid)` triples sorted by src.
    pub fn compact_forward(&self, base_csr: &AdjacencyCSR) -> Vec<(u32, u32, u64)> {
        let mut edges = Vec::new();

        // Collect surviving base edges.
        for v in 0..base_csr.num_vertices() {
            let nbrs = base_csr.neighbors(v);
            let eids = base_csr.edge_rowids_for(v);
            for (i, &dst) in nbrs.iter().enumerate() {
                let eid = eids[i];
                if !self.is_deleted(eid) {
                    edges.push((v, dst, eid));
                }
            }
        }

        // Append added edges.
        for edge in &self.added_edges {
            if !self.is_deleted(edge.edge_rowid) {
                edges.push((edge.src, edge.dst, edge.edge_rowid));
            }
        }

        edges
    }

    /// Reset the delta buffer after a successful compaction.
    pub fn clear(&mut self) {
        self.added_edges.clear();
        self.added_forward.clear();
        self.added_backward.clear();
        self.deleted_edge_rowids.clear();
    }
}

impl Default for DeltaAdjacency {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a small CSR and vertex map for testing.
    ///
    /// Graph: 0→1, 0→2, 1→2 (3 vertices, 3 edges with rowids 100,101,102)
    fn test_csr_and_vmap() -> (AdjacencyCSR, VertexIdMap) {
        let mut edges = vec![(0u32, 1u32, 100u64), (0, 2, 101), (1, 2, 102)];
        let csr = AdjacencyCSR::build(&mut edges, 3);
        let vmap = VertexIdMap::build(vec![
            (VertexKey::Int64(10), 0),
            (VertexKey::Int64(20), 1),
            (VertexKey::Int64(30), 2),
        ]);
        (csr, vmap)
    }

    #[test]
    fn empty_delta_is_transparent() {
        let (csr, _vmap) = test_csr_and_vmap();
        let delta = DeltaAdjacency::new();
        assert!(delta.is_empty());

        let nbrs = delta.neighbors_merged_forward(0, &csr);
        assert_eq!(nbrs.len(), 2); // 0→1, 0→2
        assert!(nbrs.contains(&(1, 100)));
        assert!(nbrs.contains(&(2, 101)));
    }

    #[test]
    fn add_edge_appears_in_forward_query() {
        let (csr, vmap) = test_csr_and_vmap();
        let mut delta = DeltaAdjacency::new();

        // Add edge 2→0 with rowid 200
        delta
            .add_edge(
                &VertexKey::Int64(30),
                &VertexKey::Int64(10),
                200,
                &vmap,
                &vmap,
            )
            .unwrap();

        assert_eq!(delta.added_count(), 1);
        assert!(!delta.is_empty());

        // Vertex 2 originally has no forward neighbors in the CSR.
        let nbrs = delta.neighbors_merged_forward(2, &csr);
        assert_eq!(nbrs.len(), 1);
        assert_eq!(nbrs[0], (0, 200));
    }

    #[test]
    fn add_edge_appears_in_backward_query() {
        // Build a backward CSR for edges: original edge 1→0 becomes
        // backward entry (0, 1, 100), meaning vertex 0 has backward
        // neighbor 1.
        let mut bwd_edges = vec![(0u32, 1u32, 100u64)];
        let bwd_csr = AdjacencyCSR::build(&mut bwd_edges, 3);

        let mut delta = DeltaAdjacency::new();

        // Add edge 2→0 (backward: vertex 0 gets a new backward neighbor 2)
        delta.add_edge_by_local_id(2, 0, 200);

        let nbrs = delta.neighbors_merged_backward(0, &bwd_csr);
        // Base backward CSR: vertex 0 → [1] (rowid 100)
        // Delta backward: vertex 0 → [2] (rowid 200)
        assert_eq!(nbrs.len(), 2);
        assert!(nbrs.contains(&(1, 100)));
        assert!(nbrs.contains(&(2, 200)));
    }

    #[test]
    fn delete_edge_hides_from_query() {
        let (csr, _vmap) = test_csr_and_vmap();
        let mut delta = DeltaAdjacency::new();

        // Delete edge with rowid 100 (0→1)
        delta.delete_edge(100);
        assert_eq!(delta.deleted_count(), 1);
        assert!(!delta.is_empty());
        assert!(delta.is_deleted(100));
        assert!(!delta.is_deleted(101));

        let nbrs = delta.neighbors_merged_forward(0, &csr);
        assert_eq!(nbrs.len(), 1); // only 0→2 survives
        assert_eq!(nbrs[0], (2, 101));
    }

    #[test]
    fn add_then_delete_same_edge() {
        let (csr, _vmap) = test_csr_and_vmap();
        let mut delta = DeltaAdjacency::new();

        // Add edge 2→0 with rowid 200, then delete it
        delta.add_edge_by_local_id(2, 0, 200);
        delta.delete_edge(200);

        let nbrs = delta.neighbors_merged_forward(2, &csr);
        assert!(nbrs.is_empty()); // added then deleted → invisible
    }

    #[test]
    fn compact_forward_merges_correctly() {
        let (csr, _vmap) = test_csr_and_vmap();
        let mut delta = DeltaAdjacency::new();

        // Delete 0→1 (rowid 100), add 2→0 (rowid 200)
        delta.delete_edge(100);
        delta.add_edge_by_local_id(2, 0, 200);

        let compacted = delta.compact_forward(&csr);
        // Surviving base: (0,2,101), (1,2,102)
        // Added: (2,0,200)
        assert_eq!(compacted.len(), 3);
        assert!(compacted.contains(&(0, 2, 101)));
        assert!(compacted.contains(&(1, 2, 102)));
        assert!(compacted.contains(&(2, 0, 200)));
        // Deleted (0,1,100) should NOT be present
        assert!(!compacted.iter().any(|e| e.2 == 100));
    }

    #[test]
    fn clear_resets_delta() {
        let mut delta = DeltaAdjacency::new();
        delta.add_edge_by_local_id(0, 1, 100);
        delta.delete_edge(200);
        assert!(!delta.is_empty());

        delta.clear();
        assert!(delta.is_empty());
        assert_eq!(delta.added_count(), 0);
        assert_eq!(delta.deleted_count(), 0);
    }

    #[test]
    fn add_edge_with_unknown_key_returns_error() {
        let vmap = VertexIdMap::build(vec![(VertexKey::Int64(10), 0)]);
        let mut delta = DeltaAdjacency::new();

        let result = delta.add_edge(
            &VertexKey::Int64(999), // unknown
            &VertexKey::Int64(10),
            100,
            &vmap,
            &vmap,
        );
        assert!(result.is_err());
    }

    #[test]
    fn memory_usage_increases_with_adds() {
        let mut delta = DeltaAdjacency::new();
        let base = delta.memory_usage();

        for i in 0..100u32 {
            delta.add_edge_by_local_id(i, i + 1, i as u64);
        }
        assert!(delta.memory_usage() > base);
    }

    #[test]
    fn compact_then_rebuild_csr() {
        let (csr, _vmap) = test_csr_and_vmap();
        let mut delta = DeltaAdjacency::new();

        delta.delete_edge(100); // remove 0→1
        delta.add_edge_by_local_id(2, 0, 200); // add 2→0

        let mut compacted_edges = delta.compact_forward(&csr);
        let new_csr = AdjacencyCSR::build(&mut compacted_edges, 3);

        // Verify the new CSR
        assert_eq!(new_csr.neighbors(0), &[2]); // 0→2 only (0→1 deleted)
        assert_eq!(new_csr.neighbors(1), &[2]); // 1→2 unchanged
        assert_eq!(new_csr.neighbors(2), &[0]); // 2→0 added

        // After compaction, clear delta
        delta.clear();
        assert!(delta.is_empty());
    }

    #[test]
    fn delete_edge_supports_u64_rowids_beyond_u32() {
        let mut delta = DeltaAdjacency::new();
        let large_rowid = u64::from(u32::MAX) + 7;

        delta.delete_edge(large_rowid);

        assert!(delta.is_deleted(large_rowid));
        assert_eq!(delta.deleted_count(), 1);
    }
}
