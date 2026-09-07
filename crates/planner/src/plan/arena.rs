// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Immutable logical nodes with arena-owned storage and index-only edges.
//!
//! Publishing a rewrite appends nodes and returns a new root. Existing roots
//! retain their semantics, so alternatives share unchanged subgraphs without
//! cloning. Rollback removes unpublished slots without reusing their identity.

use super::{NodeStats, OwnedLogicalPlan, PlanNodeId};
use crate::operator::{LogicalOperator, LogicalOutputLayout};
use paro_common::error::{self as paro_error, Result};
use std::sync::Arc;

/// One immutable logical DAG and its selected root. Rewrites append nodes and
/// change a root handle; they never mutate a published input or own its edges.
/// Owned IR conversion is an explicit boundary, not an implicit Clone/Deref.
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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct LogicalPlanArena {
    identity: u64,
    nodes: Vec<Slot>,
    next_generation: u64,
}

impl Default for LogicalPlanArena {
    fn default() -> Self {
        static NEXT_ARENA: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let identity = NEXT_ARENA
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |value| value.checked_add(1),
            )
            .expect("logical arena identity exhausted");
        Self {
            identity,
            nodes: Vec::new(),
            next_generation: 0,
        }
    }
}

impl LogicalPlanArena {
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
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("logical arena exhausted its revision domain"))?;
        self.next_generation = generation;
        let mut inputs = Vec::new();
        node.operator.visit_child_links(&mut |child| {
            inputs.push(self.nodes[child.slot as usize].output.clone())
        });
        let output = node.operator.output_layout_from_children(
            &inputs
                .iter()
                .map(|layout| layout.as_ref().clone())
                .collect::<Vec<_>>(),
        );
        // Row-preserving shells share a schema allocation as well as edges.
        let output = inputs
            .iter()
            .find(|input| input.as_ref() == &output)
            .cloned()
            .unwrap_or_else(|| Arc::new(output));
        self.nodes.push(Slot {
            generation,
            node,
            output,
        });
        Ok(PlanIndex {
            arena: self.identity,
            slot,
            generation,
        })
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
        self.nodes.truncate(checkpoint.len);
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
