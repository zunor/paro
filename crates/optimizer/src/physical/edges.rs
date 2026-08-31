use crate::physical::identity::Fingerprint;
use crate::physical::ids::PhysicalPlanNodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PhysicalEdgeId(pub u32);

impl PhysicalEdgeId {
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalEdgeKind {
    Data,
    Control,
    RuntimeFilter(Fingerprint),
    SharedSpool(Fingerprint),
    FixpointFeedback(Fingerprint),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalEdge {
    pub id: PhysicalEdgeId,
    pub producer: PhysicalPlanNodeId,
    pub consumer: PhysicalPlanNodeId,
    pub kind: PhysicalEdgeKind,
}

#[derive(Debug, Clone, Default)]
pub struct PhysicalEdgeArena {
    edges: Vec<PhysicalEdge>,
}

impl PhysicalEdgeArena {
    pub fn push(
        &mut self,
        producer: PhysicalPlanNodeId,
        consumer: PhysicalPlanNodeId,
        kind: PhysicalEdgeKind,
    ) -> PhysicalEdgeId {
        let id = PhysicalEdgeId(self.edges.len() as u32);
        self.edges.push(PhysicalEdge {
            id,
            producer,
            consumer,
            kind,
        });
        id
    }

    pub fn get(&self, id: PhysicalEdgeId) -> Option<&PhysicalEdge> {
        self.edges.get(id.index())
    }

    pub fn iter(&self) -> impl Iterator<Item = &PhysicalEdge> {
        self.edges.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    pub(crate) fn retain_remapped(
        &mut self,
        reachable: &[bool],
        node_remap: &[PhysicalPlanNodeId],
    ) -> Vec<Option<PhysicalEdgeId>> {
        let old_edges = std::mem::take(&mut self.edges);
        let mut edge_remap = vec![None; old_edges.len()];
        for mut edge in old_edges {
            let keep = reachable
                .get(edge.producer.index())
                .copied()
                .unwrap_or(false)
                && reachable
                    .get(edge.consumer.index())
                    .copied()
                    .unwrap_or(false);
            if !keep {
                continue;
            }
            let old_id = edge.id;
            edge.id = PhysicalEdgeId(self.edges.len() as u32);
            edge.producer = node_remap[edge.producer.index()];
            edge.consumer = node_remap[edge.consumer.index()];
            edge_remap[old_id.index()] = Some(edge.id);
            self.edges.push(edge);
        }
        edge_remap
    }
}
