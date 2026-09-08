// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable logical nodes with arena-owned storage and index-only edges.
//!
//! Publishing a rewrite appends nodes and returns a new root. Existing roots
//! retain their semantics, so alternatives *within one arena* share unchanged
//! subgraphs without cloning. The slot vector is copy-on-write: a cheap arena
//! handle can be retained by a published `LogicalPlan` while a planning
//! session continues appending new alternatives. Rollback removes unpublished
//! slots without reusing their identity.

use super::{NodeStats, OwnedLogicalPlan, PlanNodeId};
use crate::operator::{LogicalOperator, LogicalOutputLayout};
use paro_common::error::{self as paro_error, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// One immutable logical DAG and its selected root. Rewrites append nodes and
/// change a root handle; they never mutate a published input or own its edges.
/// A planner session may retain one arena and exchange only `PlanIndex` roots
/// to share subgraphs across alternatives. Owned IR conversion is an explicit
/// boundary, not an implicit Clone/Deref.
#[derive(Debug)]
pub struct LogicalPlan {
    arena: LogicalPlanArena,
    root: PlanIndex,
}

impl LogicalPlan {
    pub fn new(arena: LogicalPlanArena, root: PlanIndex) -> Result<Self> {
        arena.get(root)?;
        Ok(Self { arena, root })
    }

    pub fn from_owned(plan: OwnedLogicalPlan) -> Result<Self> {
        let mut arena = LogicalPlanArena::default();
        let root = arena.import(plan)?;
        Self::new(arena, root)
    }

    pub fn arena(&self) -> &LogicalPlanArena {
        &self.arena
    }
    pub fn root(&self) -> PlanIndex {
        self.root
    }
    pub fn root_node(&self) -> &LogicalPlanNode {
        self.arena
            .get(self.root)
            .expect("logical root is arena-owned")
    }
    pub fn output_layout(&self) -> &LogicalOutputLayout {
        self.arena
            .output_layout(self.root)
            .expect("logical root is arena-owned")
    }
    pub fn into_parts(self) -> (LogicalPlanArena, PlanIndex) {
        (self.arena, self.root)
    }

    pub fn append_root(&mut self, node: LogicalPlanNode) -> Result<()> {
        self.root = self.arena.append(node)?;
        Ok(())
    }

    pub fn into_owned(self) -> Result<OwnedLogicalPlan> {
        self.arena.export(self.root)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlanIndex {
    arena: u64,
    slot: u32,
    generation: u64,
}

#[derive(Debug, Clone)]
pub struct LogicalPlanNode<Child = PlanIndex> {
    pub id: PlanNodeId,
    pub stats: NodeStats,
    pub operator: LogicalOperator<Child>,
}

impl LogicalPlanNode<()> {
    pub fn detach(plan: OwnedLogicalPlan) -> (Self, Vec<Box<OwnedLogicalPlan>>) {
        let (id, stats, operator) = plan.into_parts();
        let mut children = Vec::new();
        let operator = operator
            .try_map_child_links(&mut |child| {
                children.push(child);
                Ok::<_, std::convert::Infallible>(())
            })
            .expect("detaching child links cannot fail");
        (
            Self {
                id,
                stats,
                operator,
            },
            children,
        )
    }

    pub fn assemble(
        self,
        children: impl IntoIterator<Item = Box<OwnedLogicalPlan>>,
    ) -> Result<OwnedLogicalPlan> {
        let mut children = children.into_iter();
        let operator = self.operator.try_map_child_links(&mut |_| {
            children
                .next()
                .ok_or_else(|| paro_error::internal("logical shell is missing an input"))
        })?;
        if children.next().is_some() {
            return Err(paro_error::internal("logical shell received excess inputs"));
        }
        Ok(OwnedLogicalPlan {
            id: self.id,
            stats: self.stats,
            operator,
        })
    }

    pub fn from_shell(plan: OwnedLogicalPlan) -> Self {
        let (id, stats, operator) = plan.into_parts();
        let operator = operator
            .try_map_child_links(&mut |_| Ok::<_, std::convert::Infallible>(()))
            .expect("detaching child ownership cannot fail");
        Self {
            id,
            stats,
            operator,
        }
    }

    pub fn instantiate(
        &self,
        id: PlanNodeId,
        children: impl IntoIterator<Item = OwnedLogicalPlan>,
    ) -> Result<OwnedLogicalPlan> {
        let mut children = children.into_iter();
        let operator = self.operator.clone().try_map_child_links(&mut |_| {
            children
                .next()
                .map(Box::new)
                .ok_or_else(|| paro_error::internal("logical shell is missing an input"))
        })?;
        if children.next().is_some() {
            return Err(paro_error::internal("logical shell received excess inputs"));
        }
        Ok(OwnedLogicalPlan {
            id,
            stats: self.stats.clone(),
            operator,
        })
    }
}

#[derive(Debug, Clone)]
struct Slot {
    generation: u64,
    node: LogicalPlanNode,
    output: Arc<LogicalOutputLayout>,
}

#[derive(Debug, Clone, Copy)]
pub struct PlanArenaCheckpoint {
    arena: u64,
    len: usize,
    prefix_generation: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct LogicalPlanArena {
    identity: u64,
    nodes: Arc<Vec<Arc<Slot>>>,
    /// Generation allocation is shared by copy-on-write handles.  A frozen
    /// alternative may append a suffix while the session arena continues to
    /// grow; sharing this counter prevents both branches from manufacturing
    /// the same `(arena, slot, generation)` index for different nodes.
    next_generation: Arc<AtomicU64>,
}

impl Default for LogicalPlanArena {
    fn default() -> Self {
        static NEXT_ARENA: AtomicU64 = AtomicU64::new(1);
        let identity = NEXT_ARENA
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .expect("logical arena identity exhausted");
        Self {
            identity,
            nodes: Arc::new(Vec::new()),
            next_generation: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl LogicalPlanArena {
    /// Whether an index belongs to this arena generation domain.  A session
    /// can use this to retain an existing root directly instead of absorbing
    /// (and potentially cloning) an arena that is already shared by handle.
    pub fn owns(&self, index: PlanIndex) -> bool {
        index.arena == self.identity && self.get(index).is_ok()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn get(&self, index: PlanIndex) -> Result<&LogicalPlanNode> {
        if index.arena != self.identity {
            return Err(paro_error::internal(
                "logical reference belongs to another arena",
            ));
        }
        self.nodes
            .get(index.slot as usize)
            .filter(|slot| slot.generation == index.generation)
            .map(|slot| &slot.node)
            .ok_or_else(|| paro_error::internal("logical arena reference is stale or unknown"))
    }

    pub fn output_layout(&self, index: PlanIndex) -> Result<&LogicalOutputLayout> {
        self.get(index)?;
        Ok(&self.nodes[index.slot as usize].output)
    }

    pub fn append(&mut self, node: LogicalPlanNode) -> Result<PlanIndex> {
        let mut valid = true;
        node.operator
            .visit_child_links(&mut |child| valid &= self.get(*child).is_ok());
        if !valid {
            return Err(paro_error::internal(
                "logical node contains a stale arena edge",
            ));
        }
        let slot = u32::try_from(self.nodes.len())
            .map_err(|_| paro_error::internal("logical arena exhausted its index domain"))?;
        let generation = self
            .next_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| paro_error::internal("logical arena exhausted its revision domain"))?
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("logical arena exhausted its revision domain"))?;
        let mut inputs = Vec::new();
        node.operator.visit_child_links(&mut |child| {
            inputs.push(self.nodes[child.slot as usize].output.clone())
        });
        let input_refs = inputs.iter().map(Arc::as_ref).collect::<Vec<_>>();
        // Row-preserving shells share a schema allocation as well as edges.
        // The operator contract tells us the source directly; do not allocate
        // and compare a full layout just to rediscover that fact.
        let output = node
            .operator
            .pass_through_child_index(&input_refs)
            .and_then(|index| inputs.get(index).cloned())
            .unwrap_or_else(|| Arc::new(node.operator.output_layout_from_child_refs(&input_refs)));
        Arc::make_mut(&mut self.nodes).push(Arc::new(Slot {
            generation,
            node,
            output,
        }));
        Ok(PlanIndex {
            arena: self.identity,
            slot,
            generation,
        })
    }

    /// Move an arena into a planning-session arena without cloning operator
    /// payloads.  Child indices are remapped once while slots and their
    /// already-derived layouts are transferred by ownership.  This is the
    /// ownership boundary used by Memo staging: alternatives from one query
    /// now retain a single session arena instead of allocating a fresh arena
    /// for every transformed expression.
    pub fn absorb(&mut self, other: LogicalPlanArena, root: PlanIndex) -> Result<PlanIndex> {
        if root.arena != other.identity {
            return Err(paro_error::internal(
                "logical arena absorb received a root from another arena",
            ));
        }
        let source_nodes = Arc::try_unwrap(other.nodes).unwrap_or_else(|nodes| (*nodes).clone());
        let mut remapped: Vec<PlanIndex> = Vec::with_capacity(source_nodes.len());
        for slot in source_nodes {
            let slot = Arc::try_unwrap(slot).unwrap_or_else(|slot| (*slot).clone());
            let generation = self
                .next_generation
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| paro_error::internal("logical arena exhausted its revision domain"))?
                .checked_add(1)
                .ok_or_else(|| {
                    paro_error::internal("logical arena exhausted its revision domain")
                })?;
            let operator = slot.node.operator.try_map_child_links(&mut |child| {
                remapped
                    .get(child.slot as usize)
                    .copied()
                    .ok_or_else(|| paro_error::internal("logical arena absorb lost a child"))
            })?;
            let slot_index = u32::try_from(self.nodes.len())
                .map_err(|_| paro_error::internal("logical arena exhausted its index domain"))?;
            let index = PlanIndex {
                arena: self.identity,
                slot: slot_index,
                generation,
            };
            Arc::make_mut(&mut self.nodes).push(Arc::new(Slot {
                generation,
                node: LogicalPlanNode {
                    id: slot.node.id,
                    stats: slot.node.stats,
                    operator,
                },
                output: slot.output,
            }));
            remapped.push(index);
        }
        remapped
            .get(root.slot as usize)
            .copied()
            .ok_or_else(|| paro_error::internal("logical arena absorb lost its root"))
    }

    /// Adopt a root from an arena handle that shares this session's identity.
    /// A published `LogicalPlan` may have appended a frozen root through
    /// copy-on-write, leaving the session with the same immutable prefix and
    /// a short suffix.  Reattach only that suffix; cloning the prefix would
    /// defeat the arena's ownership contract.
    pub fn adopt_or_absorb(
        &mut self,
        other: LogicalPlanArena,
        root: PlanIndex,
    ) -> Result<PlanIndex> {
        if other.identity != self.identity {
            return self.absorb(other, root);
        }
        // A root that is already live in the session is unambiguous because
        // COW handles share a global generation allocator.  This is the
        // common path for a settled alternative whose suffix was published
        // before another alternative was prepared.
        if self.owns(root) {
            return Ok(root);
        }
        if self.len() <= other.len()
            && self
                .nodes
                .iter()
                .zip(other.nodes.iter())
                .all(|(left, right)| Arc::ptr_eq(left, right))
        {
            let suffix = other
                .nodes
                .iter()
                .skip(self.len())
                .cloned()
                .collect::<Vec<_>>();
            Arc::make_mut(&mut self.nodes).extend(suffix);
            if self.owns(root) {
                return Ok(root);
            }
        }
        // The two handles grew divergent suffixes.  Rebase the complete
        // snapshot once, remapping edges and preserving the session identity;
        // this is slower than suffix adoption but remains correct even when a
        // later alternative occupied the same slot in the session vector.
        self.absorb(other, root)
    }

    pub fn checkpoint(&self) -> PlanArenaCheckpoint {
        PlanArenaCheckpoint {
            arena: self.identity,
            len: self.nodes.len(),
            prefix_generation: self.nodes.last().map(|slot| slot.generation),
        }
    }

    pub fn rollback_to(&mut self, checkpoint: PlanArenaCheckpoint) -> Result<()> {
        if checkpoint.arena != self.identity
            || checkpoint.len > self.nodes.len()
            || checkpoint.prefix_generation
                != checkpoint
                    .len
                    .checked_sub(1)
                    .and_then(|index| self.nodes.get(index).map(|slot| slot.generation))
        {
            return Err(paro_error::internal(
                "logical arena checkpoint is no longer reachable",
            ));
        }
        Arc::make_mut(&mut self.nodes).truncate(checkpoint.len);
        Ok(())
    }

    /// Transfer a binder-owned tree into the arena once. No placeholder node
    /// or replacement child Box is allocated while detaching its edges.
    pub fn import(&mut self, plan: OwnedLogicalPlan) -> Result<PlanIndex> {
        self.import_checked(plan, || Ok(()))
    }

    /// The caller admits/cancels each node before its shell is detached or
    /// allocated. Failed transfers roll back without reviving old identities.
    pub fn import_checked(
        &mut self,
        plan: OwnedLogicalPlan,
        mut admit: impl FnMut() -> Result<()>,
    ) -> Result<PlanIndex> {
        enum Frame {
            Enter(OwnedLogicalPlan),
            Exit(PlanNodeId, NodeStats, LogicalOperator<()>, usize),
        }
        let checkpoint = self.checkpoint();
        let result = (|| {
            let mut pending = vec![Frame::Enter(plan)];
            let mut completed = Vec::new();
            while let Some(frame) = pending.pop() {
                match frame {
                    Frame::Enter(plan) => {
                        admit()?;
                        let (id, stats, operator) = plan.into_parts();
                        let mut children = Vec::new();
                        let shell = operator.try_map_child_links(&mut |child| {
                            children.push(*child);
                            Ok::<_, paro_common::error::ParoError>(())
                        })?;
                        pending.push(Frame::Exit(id, stats, shell, children.len()));
                        pending.extend(children.into_iter().rev().map(Frame::Enter));
                    }
                    Frame::Exit(id, stats, shell, arity) => {
                        let start = completed.len().checked_sub(arity).ok_or_else(|| {
                            paro_error::internal("logical arena import lost a child")
                        })?;
                        let mut cursor = start;
                        let operator = shell.try_map_child_links(&mut |_| {
                            let child = completed[cursor];
                            cursor += 1;
                            Ok::<_, paro_common::error::ParoError>(child)
                        })?;
                        completed.truncate(start);
                        completed.push(self.append(LogicalPlanNode {
                            id,
                            stats,
                            operator,
                        })?);
                    }
                }
            }
            if completed.len() != 1 {
                return Err(paro_error::internal(
                    "logical arena import has no unique root",
                ));
            }
            Ok(completed[0])
        })();
        if result.is_err() {
            self.rollback_to(checkpoint)?;
        }
        result
    }

    /// Read the reachable DAG in topological order, visiting each shared node
    /// exactly once. Append-only edges always point to an earlier slot.
    pub fn post_order(&self, root: PlanIndex) -> Result<Vec<PlanIndex>> {
        self.post_order_checked(root, || Ok(()))
    }

    /// Admission/cancellation also covers reading an existing DAG, not just
    /// constructing its nodes. Check every edge before growing the read set.
    pub fn post_order_checked(
        &self,
        root: PlanIndex,
        mut admit: impl FnMut() -> Result<()>,
    ) -> Result<Vec<PlanIndex>> {
        let mut visited = std::collections::BTreeSet::new();
        let mut pending = vec![root];
        while let Some(index) = pending.pop() {
            admit()?;
            let node = self.get(index)?;
            if visited.insert(index) {
                node.operator
                    .visit_child_links(&mut |child| pending.push(*child));
            }
        }
        Ok(visited.into_iter().collect())
    }

    /// Materialize evaluation occurrences at an ownership boundary. Traversal
    /// is iterative; shared definitions are intentionally not execution CSE.
    pub fn export(&self, root: PlanIndex) -> Result<OwnedLogicalPlan> {
        self.export_checked(root, || Ok(()))
    }

    /// Export is deliberately an occurrence operation. A caller crossing an
    /// owned-IR boundary can bound/cancel it before every repeated DAG node;
    /// checking only distinct node count would not bound a diamond expansion.
    pub fn export_checked(
        &self,
        root: PlanIndex,
        mut admit: impl FnMut() -> Result<()>,
    ) -> Result<OwnedLogicalPlan> {
        enum Frame {
            Enter(PlanIndex),
            Exit(LogicalPlanNode<()>, usize),
        }
        let mut pending = vec![Frame::Enter(root)];
        let mut completed = Vec::new();
        while let Some(frame) = pending.pop() {
            match frame {
                Frame::Enter(index) => {
                    admit()?;
                    let node = self.get(index)?.clone();
                    let mut children = Vec::new();
                    let operator = node.operator.try_map_child_links(&mut |child| {
                        children.push(child);
                        Ok::<_, paro_common::error::ParoError>(())
                    })?;
                    pending.push(Frame::Exit(
                        LogicalPlanNode {
                            id: node.id,
                            stats: node.stats,
                            operator,
                        },
                        children.len(),
                    ));
                    pending.extend(children.into_iter().rev().map(Frame::Enter));
                }
                Frame::Exit(node, arity) => {
                    let start = completed.len().checked_sub(arity).ok_or_else(|| {
                        paro_error::internal("logical arena export lost an input")
                    })?;
                    let children = completed.drain(start..).collect::<Vec<_>>();
                    completed.push(Box::new(node.assemble(children)?));
                }
            }
        }
        if completed.len() != 1 {
            return Err(paro_error::internal("logical arena export lost its root"));
        }
        Ok(*completed.pop().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::{CrossProduct, Filter, Join};

    fn identity() -> LogicalPlanNode {
        LogicalPlanNode {
            id: PlanNodeId::SYNTHETIC,
            stats: NodeStats::default(),
            operator: LogicalOperator::DummyScan,
        }
    }

    #[test]
    fn shared_dag_uses_index_edges_and_visits_each_node_once() {
        let mut arena = LogicalPlanArena::default();
        let mut root = arena.append(identity()).unwrap();
        for _ in 0..32 {
            root = arena
                .append(LogicalPlanNode {
                    id: PlanNodeId::SYNTHETIC,
                    stats: NodeStats::default(),
                    operator: LogicalOperator::Join(Join::Cross(CrossProduct {
                        left: root,
                        right: root,
                        build_side_constraint: crate::operator::JoinBuildSideConstraint::default(),
                    })),
                })
                .unwrap();
        }
        assert_eq!(arena.len(), 33);
        assert_eq!(arena.post_order(root).unwrap().len(), 33);
    }

    #[test]
    fn pass_through_nodes_reuse_the_child_layout_arc() {
        let mut arena = LogicalPlanArena::default();
        let child = arena.append(identity()).unwrap();
        let filter = arena
            .append(LogicalPlanNode {
                id: PlanNodeId::SYNTHETIC,
                stats: NodeStats::default(),
                operator: LogicalOperator::Filter(Filter {
                    child,
                    expressions: vec![],
                    projection_map: crate::operator::ProjectionMap::all(),
                }),
            })
            .unwrap();
        assert!(Arc::ptr_eq(
            &arena.nodes[child.slot as usize].output,
            &arena.nodes[filter.slot as usize].output,
        ));
    }

    #[test]
    fn rollback_and_reinsert_never_revives_a_stale_edge() {
        let mut arena = LogicalPlanArena::default();
        let root = arena.append(identity()).unwrap();
        let checkpoint = arena.checkpoint();
        let removed = arena.append(identity()).unwrap();
        arena.rollback_to(checkpoint).unwrap();
        let replacement = arena.append(identity()).unwrap();
        assert_ne!(removed, replacement);
        assert!(arena.get(removed).is_err());
        assert!(arena.get(root).is_ok());
        assert!(arena
            .append(LogicalPlanNode {
                id: PlanNodeId::SYNTHETIC,
                stats: NodeStats::default(),
                operator: LogicalOperator::Filter(Filter {
                    child: removed,
                    expressions: vec![],
                    projection_map: crate::operator::ProjectionMap::all()
                })
            })
            .is_err());
    }

    #[test]
    fn absorb_moves_a_plan_without_cloning_its_slots() {
        let mut source = LogicalPlanArena::default();
        let root = source.append(identity()).unwrap();
        let mut session = LogicalPlanArena::default();
        let moved = session.absorb(source, root).unwrap();
        assert_eq!(session.len(), 1);
        assert_ne!(moved, root);
        assert!(session.get(moved).is_ok());
    }

    #[test]
    fn cloned_arena_handles_share_slots_until_an_append() {
        let mut arena = LogicalPlanArena::default();
        let root = arena.append(identity()).unwrap();
        let snapshot = arena.clone();
        assert!(Arc::ptr_eq(&arena.nodes, &snapshot.nodes));

        // Publishing an alternative is copy-on-write. The old root remains
        // valid while the retained snapshot continues to observe the
        // published prefix without cloning its operator payloads.
        let _next = arena
            .append(LogicalPlanNode {
                id: PlanNodeId::SYNTHETIC,
                stats: NodeStats::default(),
                operator: LogicalOperator::Filter(Filter {
                    child: root,
                    expressions: vec![],
                    projection_map: crate::operator::ProjectionMap::all(),
                }),
            })
            .unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.post_order(root).unwrap(), vec![root]);
        assert_eq!(arena.len(), 2);
    }

    #[test]
    fn same_identity_adoption_moves_only_copy_on_write_suffix() {
        let mut arena = LogicalPlanArena::default();
        let root = arena.append(identity()).unwrap();
        let mut fork = arena.clone();
        let suffix = fork
            .append(LogicalPlanNode {
                id: PlanNodeId::SYNTHETIC,
                stats: NodeStats::default(),
                operator: LogicalOperator::Filter(Filter {
                    child: root,
                    expressions: vec![],
                    projection_map: crate::operator::ProjectionMap::all(),
                }),
            })
            .unwrap();
        let adopted = arena.adopt_or_absorb(fork, suffix).unwrap();
        assert_eq!(adopted, suffix);
        assert_eq!(arena.len(), 2);
        assert!(arena.owns(adopted));
    }

    #[test]
    fn import_is_stack_safe_and_preserves_every_node_without_placeholders() {
        let mut plan = OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan);
        for _ in 0..8192 {
            plan = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(plan, vec![])));
        }
        let mut arena = LogicalPlanArena::default();
        let root = arena.import(plan).unwrap();
        assert_eq!(arena.len(), 8193);
        assert_eq!(arena.post_order(root).unwrap().len(), 8193);
        let exported = arena.export(root).unwrap();
        let mut visits = 0;
        exported
            .try_visit_pre_order(|_| {
                visits += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(visits, 8193);
    }

    #[test]
    fn occurrence_export_admits_before_expanding_a_shared_dag() {
        let mut arena = LogicalPlanArena::default();
        let mut root = arena.append(identity()).unwrap();
        for _ in 0..32 {
            root = arena
                .append(LogicalPlanNode {
                    operator: LogicalOperator::Join(Join::Cross(CrossProduct {
                        left: root,
                        right: root,
                        build_side_constraint: Default::default(),
                    })),
                    ..identity()
                })
                .unwrap();
        }
        let mut admitted = 0;
        let result = arena.export_checked(root, || {
            if admitted == 8 {
                return Err(paro_error::internal("test work limit"));
            }
            admitted += 1;
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(admitted, 8);
        assert_eq!(arena.len(), 33);
    }

    #[test]
    fn cancelled_import_preserves_published_roots() {
        let mut arena = LogicalPlanArena::default();
        let published = arena.append(identity()).unwrap();
        let mut visits = 0;
        let plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::Join(Join::Cross(CrossProduct::new(
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
            ))));
        assert!(arena
            .import_checked(plan, || {
                visits += 1;
                if visits == 3 {
                    Err(paro_error::internal("cancelled"))
                } else {
                    Ok(())
                }
            })
            .is_err());
        assert_eq!(arena.len(), 1);
        assert!(arena.get(published).is_ok());
    }
}
