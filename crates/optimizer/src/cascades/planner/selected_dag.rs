// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Exact selected structure, independent of fact revisions and certification.
//! Each visited candidate owns one immutable edge list. Only the current root
//! has a materialized view; retaining every root's transitive closure would
//! turn a shared chain back into quadratic storage.

use super::*;

/// A borrowed lookup over the one selected-DAG index. Standalone producers
/// (including the independent oracle) may own an index, but property consumers
/// never materialize another map for the same graph.
pub(super) struct QualityNodeMap<'a> {
    nodes: &'a [QualityCandidateNode],
    index: std::borrow::Cow<'a, BTreeMap<CandidateId, usize>>,
}

impl<'a> QualityNodeMap<'a> {
    pub fn from_nodes(nodes: &'a [QualityCandidateNode]) -> Self {
        Self {
            nodes,
            index: std::borrow::Cow::Owned(
                nodes
                    .iter()
                    .enumerate()
                    .map(|(i, node)| (node.reference.candidate, i))
                    .collect(),
            ),
        }
    }

    pub fn get(&self, candidate: &CandidateId) -> Option<&'a QualityCandidateNode> {
        self.index.get(candidate).map(|index| &self.nodes[*index])
    }
}

#[derive(Debug)]
pub(super) struct SelectedDag {
    pub root: ChildWinnerRef,
    pub nodes: Arc<[QualityCandidateNode]>,
    pub postorder: Box<[usize]>,
    pub parents: Box<[Box<[usize]>]>,
    index: BTreeMap<CandidateId, usize>,
}

impl SelectedDag {
    pub fn incoming(&self, candidate: CandidateId) -> impl Iterator<Item = &QualityCandidateNode> {
        self.index
            .get(&candidate)
            .into_iter()
            .flat_map(|index| self.parents[*index].iter())
            .map(|index| &self.nodes[*index])
    }
    pub fn node_map(&self) -> QualityNodeMap<'_> {
        QualityNodeMap {
            nodes: &self.nodes,
            index: std::borrow::Cow::Borrowed(&self.index),
        }
    }

    /// Also used by the malformed-graph tests, independently of Memo import.
    pub fn from_nodes(root: ChildWinnerRef, nodes: Arc<[QualityCandidateNode]>) -> Option<Self> {
        let index: BTreeMap<_, _> = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.reference.candidate, index))
            .collect();
        if index.len() != nodes.len() {
            return None;
        }
        let mut parents = vec![Vec::new(); nodes.len()];
        let mut colors = vec![0_u8; nodes.len()];
        let mut postorder = Vec::with_capacity(nodes.len());
        let mut pending = vec![(root, false)];
        while let Some((reference, exit)) = pending.pop() {
            let i = *index.get(&reference.candidate)?;
            let node = &nodes[i];
            if node.reference != reference {
                return None;
            }
            if exit {
                colors[i] = 2;
                postorder.push(i);
                continue;
            }
            match colors[i] {
                1 => return None,
                2 => continue,
                _ => {}
            }
            colors[i] = 1;
            pending.push((reference, true));
            for child in node.children.iter().rev() {
                let child_index = *index.get(&child.candidate)?;
                parents[child_index].push(i);
                pending.push((*child, false));
            }
        }
        if postorder.len() != nodes.len() {
            return None;
        }
        Some(Self {
            root,
            nodes,
            postorder: postorder.into_boxed_slice(),
            index,
            parents: parents.into_iter().map(Vec::into_boxed_slice).collect(),
        })
    }
}

#[derive(Debug, Default)]
pub(super) struct SelectedDagStore {
    nodes: BTreeMap<CandidateId, QualityCandidateNode>,
    selected: Option<Arc<SelectedDag>>,
}

impl SelectedDagStore {
    pub fn select(&mut self, memo: &Memo, root: ChildWinnerRef) -> Option<Arc<SelectedDag>> {
        if let Some(selected) = &self.selected {
            if selected.root == root {
                return Some(selected.clone());
            }
        }
        let mut nodes = Vec::new();
        let mut seen = BTreeSet::new();
        let mut pending = vec![root];
        while let Some(reference) = pending.pop() {
            let node = match self.nodes.entry(reference.candidate) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let winner = memo.resolve_child_winner(reference)?;
                    let physical = memo.physical_expr(winner.expression)?;
                    entry.insert(QualityCandidateNode {
                        reference,
                        logical: physical.key.logical,
                        physical: physical.id,
                        children: Arc::from(winner.children.as_ref()),
                    })
                }
            };
            if node.reference != reference {
                return None;
            }
            if !seen.insert(reference.candidate) {
                continue;
            }
            // Preserve the original inspection's traversal order exactly.
            pending.extend(node.children.iter().copied());
            nodes.push(node.clone());
        }
        let graph = Arc::new(SelectedDag::from_nodes(root, nodes.into())?);
        self.selected = Some(graph.clone());
        Some(graph)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(id: usize) -> ChildWinnerRef {
        ChildWinnerRef {
            candidate: CandidateId::new(id),
            group: GroupId::new(id),
            goal: OptimizationGoal {
                required: Default::default(),
                grant: GrantGoalKey::Invariant(Default::default()),
                row_goal: super::super::super::memo::RowGoal::All,
                objective: crate::physical::ObjectiveProfile::Latency,
                context: Default::default(),
            },
        }
    }

    fn node(id: usize, children: &[usize]) -> QualityCandidateNode {
        QualityCandidateNode {
            reference: reference(id),
            logical: LogicalExprId::new(id),
            physical: PhysicalExprId::new(id),
            children: children.iter().map(|id| reference(*id)).collect(),
        }
    }

    #[test]
    fn shared_child_retains_every_selected_incoming_edge() {
        let dag = SelectedDag::from_nodes(
            reference(0),
            Arc::from([node(0, &[1, 2]), node(1, &[3]), node(2, &[3]), node(3, &[])]),
        )
        .unwrap();
        assert_eq!(dag.postorder.len(), 4);
        let parents: BTreeSet<_> = dag
            .incoming(CandidateId::new(3))
            .map(|node| node.reference.candidate)
            .collect();
        assert_eq!(
            parents,
            BTreeSet::from([CandidateId::new(1), CandidateId::new(2)])
        );
        assert_eq!(dag.postorder.last(), Some(&0));
        let lookup = dag.node_map();
        assert!(matches!(lookup.index, std::borrow::Cow::Borrowed(_)));
        assert!(std::ptr::eq(
            lookup.get(&CandidateId::new(3)).unwrap(),
            &dag.nodes[3]
        ));
        assert!(quality_node(&lookup, reference(3)).is_some());
        let mut wrong_goal = reference(3);
        wrong_goal.goal.row_goal = super::super::super::memo::RowGoal::AtMost(1);
        assert!(quality_node(&lookup, wrong_goal).is_none());
    }

    #[test]
    fn graph_rejects_holes_cycles_conflicting_goals_and_unreachable_nodes() {
        assert!(SelectedDag::from_nodes(reference(0), Arc::from([node(0, &[1])])).is_none());
        assert!(
            SelectedDag::from_nodes(reference(0), Arc::from([node(0, &[1]), node(1, &[0])]))
                .is_none()
        );
        assert!(
            SelectedDag::from_nodes(reference(0), Arc::from([node(0, &[]), node(1, &[])]))
                .is_none()
        );
        let mut parent = node(0, &[1]);
        Arc::make_mut(&mut parent.children)[0].goal.row_goal =
            super::super::super::memo::RowGoal::AtMost(1);
        assert!(SelectedDag::from_nodes(reference(0), Arc::from([parent, node(1, &[])])).is_none());
    }

    #[test]
    fn deep_selected_graph_uses_bounded_native_stack() {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(|| {
                let mut nodes: Vec<_> = (0..10_000).map(|id| node(id, &[id + 1])).collect();
                nodes.push(node(10_000, &[]));
                let dag = SelectedDag::from_nodes(reference(0), nodes.into()).unwrap();
                assert_eq!(dag.postorder.len(), 10_001);
                assert_eq!(dag.postorder.first(), Some(&10_000));
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
