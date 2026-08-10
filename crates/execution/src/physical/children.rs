// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Compact child-list storage for physical plan nodes.

use super::ids::{PhysicalPlanNodeId, PlanChildrenId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlinePlanChildren {
    len: u8,
    ids: [PhysicalPlanNodeId; 2],
}

impl InlinePlanChildren {
    pub fn new(ids: &[PhysicalPlanNodeId]) -> Self {
        assert!(
            ids.len() <= 2,
            "inline physical plan children only supports unary/binary nodes"
        );
        let mut inline = Self {
            len: ids.len() as u8,
            ids: [PhysicalPlanNodeId::INVALID; 2],
        };
        for (idx, id) in ids.iter().enumerate() {
            inline.ids[idx] = *id;
        }
        inline
    }

    #[inline]
    pub fn as_slice(&self) -> &[PhysicalPlanNodeId] {
        &self.ids[..self.len as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanChildren {
    Empty,
    Inline(InlinePlanChildren),
    Many(PlanChildrenId),
}

#[derive(Debug, Clone, Default)]
pub struct PlanChildrenArena {
    lists: Vec<Box<[PhysicalPlanNodeId]>>,
}

impl PlanChildrenArena {
    pub fn pack(&mut self, ids: Vec<PhysicalPlanNodeId>) -> PlanChildren {
        match ids.len() {
            0 => PlanChildren::Empty,
            1 | 2 => PlanChildren::Inline(InlinePlanChildren::new(&ids)),
            _ => {
                let id = PlanChildrenId::new(self.lists.len());
                self.lists.push(ids.into_boxed_slice());
                PlanChildren::Many(id)
            }
        }
    }

    pub fn get(&self, id: PlanChildrenId) -> &[PhysicalPlanNodeId] {
        &self.lists[id.index()]
    }

    pub fn len(&self) -> usize {
        self.lists.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lists.is_empty()
    }
}

impl PlanChildren {
    pub fn as_slice<'a>(&'a self, arena: &'a PlanChildrenArena) -> &'a [PhysicalPlanNodeId] {
        match self {
            Self::Empty => &[],
            Self::Inline(children) => children.as_slice(),
            Self::Many(id) => arena.get(*id),
        }
    }

    pub(crate) fn replace_only(&mut self, child: PhysicalPlanNodeId) {
        let Self::Inline(children) = self else {
            panic!("unary physical node must store its child inline");
        };
        assert_eq!(children.len, 1, "physical node must have exactly one child");
        children.ids[0] = child;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn children_pack_keeps_unary_and_binary_inline() {
        let mut arena = PlanChildrenArena::default();
        let one = arena.pack(vec![PhysicalPlanNodeId::new(1)]);
        let two = arena.pack(vec![PhysicalPlanNodeId::new(1), PhysicalPlanNodeId::new(2)]);

        assert!(matches!(one, PlanChildren::Inline(_)));
        assert!(matches!(two, PlanChildren::Inline(_)));
        assert!(arena.is_empty());
    }

    #[test]
    fn children_pack_uses_overflow_arena_for_many_inputs() {
        let mut arena = PlanChildrenArena::default();
        let packed = arena.pack(vec![
            PhysicalPlanNodeId::new(1),
            PhysicalPlanNodeId::new(2),
            PhysicalPlanNodeId::new(3),
        ]);

        assert!(matches!(packed, PlanChildren::Many(_)));
        assert_eq!(packed.as_slice(&arena).len(), 3);
        assert_eq!(arena.len(), 1);
    }
}
