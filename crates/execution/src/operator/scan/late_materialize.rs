//! Late materialization planning and budget checks.

use std::sync::Arc;

use paro_storage::buffer::TemporaryMemoryState;
use paro_storage::index::{collect_predicate_columns, ColumnId, PredicateTree};
use paro_storage::tablet::ColumnProjection;

#[derive(Debug, Clone)]
pub struct LateMaterializePlan {
    pub enabled: bool,
    pub predicate_columns: Vec<ColumnId>,
    pub required_bytes: usize,
}

impl LateMaterializePlan {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            predicate_columns: Vec::new(),
            required_bytes: 0,
        }
    }
}

pub fn plan_late_materialization(
    predicate_tree: Option<&PredicateTree>,
    projection: &ColumnProjection,
    batch_size: usize,
    state: &Arc<TemporaryMemoryState>,
) -> LateMaterializePlan {
    if predicate_tree.is_none() || projection.read_columns().is_empty() {
        state.set_zero();
        return LateMaterializePlan::disabled();
    }

    let tree = predicate_tree.expect("predicate_tree checked");
    let predicate_columns = collect_predicate_columns(tree);
    if predicate_columns.is_empty() {
        state.set_zero();
        return LateMaterializePlan::disabled();
    }

    let required_bytes = estimate_required_bytes(batch_size);
    if required_bytes == 0 {
        state.set_zero();
        return LateMaterializePlan::disabled();
    }

    state.set_remaining_size_and_update_reservation(required_bytes);
    let enabled = state.get_reservation() >= required_bytes;

    LateMaterializePlan {
        enabled,
        predicate_columns,
        required_bytes,
    }
}

fn estimate_required_bytes(batch_size: usize) -> usize {
    let rows = batch_size.max(1);
    let rowid_bytes = rows.saturating_mul(std::mem::size_of::<u32>());
    let bitmap_bytes = (rows + 7) / 8;
    rowid_bytes.saturating_add(bitmap_bytes).max(1)
}
