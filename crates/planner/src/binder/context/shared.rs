use crate::plan::PlanNodeId;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

/// Shared allocators that remain stable across nested binder scopes.
#[derive(Debug)]
pub struct BindShared {
    next_table_index: AtomicUsize,
    next_plan_node_id: AtomicU32,
}

impl Default for BindShared {
    fn default() -> Self {
        Self {
            next_table_index: AtomicUsize::new(0),
            next_plan_node_id: AtomicU32::new(1),
        }
    }
}

impl BindShared {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn generate_table_index(&self) -> usize {
        self.next_table_index.fetch_add(1, Ordering::Relaxed)
    }

    pub fn next_plan_id(&self) -> PlanNodeId {
        PlanNodeId(self.next_plan_node_id.fetch_add(1, Ordering::Relaxed))
    }
}
