// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::plan::policy::{
    BaseCompactionPolicy, CompactionPolicy, CompactionRowsetSet, CompactionSelection,
    CumulativeCompactionPolicy, SizeTieredCompactionPolicy,
};
use crate::compaction::plan::types::{
    CompactionInput, CompactionPlan, CompactionPlanId, CompactionReason, CumulativePointAction,
    ExecutionLayout, MergeSemantics, PkDeltaGuard, PolicyKind, ReadSnapshot,
};
use crate::rowset::RowsetSharedPtr;
use crate::tablet::{KeysType, Tablet, Version};
use paro_common::error::{self as paro_error, Result};
use std::sync::atomic::{AtomicU64, Ordering};

const PK_PUBLISH_DELTA_MAX_ROWS: u64 = 5_000_000;
const PK_PUBLISH_DELTA_MAX_BYTES: u64 = 512 * 1024 * 1024;
const MAX_BASELINE_TO_NEWER_BYTES: u64 = 5;

/// Shared write-amplification admission applied after every policy has chosen
/// a valid version range.
///
/// The oldest input is the bytes being rewritten; all later inputs are the
/// progress that justifies that rewrite. Requiring at least one byte of newer
/// data per five baseline bytes bounds input write amplification at 6x without
/// relying on wall time, rowset count, or policy ordering. Size-tiered
/// compaction can still merge small deltas independently until they are large
/// enough to join an established base.
fn rewrite_amplification_admitted(rowsets: &[RowsetSharedPtr]) -> bool {
    let Some((baseline, newer)) = rowsets.split_first() else {
        return false;
    };
    if newer.is_empty() {
        return false;
    }
    let baseline_bytes = u128::from(baseline.total_disk_size());
    let newer_bytes = newer.iter().fold(0_u128, |total, rowset| {
        total.saturating_add(u128::from(rowset.total_disk_size()))
    });
    baseline_bytes <= newer_bytes.saturating_mul(u128::from(MAX_BASELINE_TO_NEWER_BYTES))
}

pub struct CompactionPlanner;

impl CompactionPlanner {
    pub fn plan(tablet: &Tablet) -> Result<Option<CompactionPlan>> {
        let Some(schema) = tablet.schema() else {
            return Ok(None);
        };
        let rowsets = CompactionRowsetSet::capture(tablet)?;

        if schema.keys_type() == KeysType::PrimaryKeys {
            return Self::plan_primary_key(tablet, &rowsets);
        }

        let size_tiered = SizeTieredCompactionPolicy::new();
        let cumulative = CumulativeCompactionPolicy::new();
        let base = BaseCompactionPolicy::new();
        let policies: [&dyn CompactionPolicy; 3] = [&size_tiered, &cumulative, &base];
        let mut selected = None;
        for policy in policies {
            if let Some(selection) = Self::select(&rowsets, policy)? {
                selected = Some(selection);
                break;
            }
        }

        let Some(CompactionSelection { decision, rowsets }) = selected else {
            return Ok(None);
        };

        let merge_semantics = match schema.keys_type() {
            KeysType::DuplicateKeys => MergeSemantics::Append,
            KeysType::AggregateKeys => MergeSemantics::Aggregate,
            KeysType::PrimaryKeys => unreachable!("PRIMARY_KEYS is handled by plan_primary_key"),
            KeysType::UniqueKeys => {
                return Err(paro_error::not_supported(
                    "UNIQUE_KEYS compaction is reserved for future KeyedPublishDelta work",
                ));
            }
        };

        let output_version = Version::new(
            rowsets
                .first()
                .map(|rowset| rowset.start_version())
                .unwrap_or_default(),
            rowsets
                .last()
                .map(|rowset| rowset.end_version())
                .unwrap_or_default(),
        );
        let input_rowsets: Vec<CompactionInput> =
            rowsets.into_iter().map(CompactionInput::new).collect();
        let pk_delta_guard = match merge_semantics {
            MergeSemantics::Deduplicate => Some(build_pk_delta_guard(tablet, &input_rowsets)?),
            _ => None,
        };
        let execution_layout = match merge_semantics {
            MergeSemantics::Deduplicate | MergeSemantics::UniqueLatest => {
                ExecutionLayout::Horizontal
            }
            _ if schema.columns().len() > 10 => ExecutionLayout::Vertical,
            _ => ExecutionLayout::Horizontal,
        };

        Ok(Some(CompactionPlan {
            plan_id: next_plan_id(),
            tablet_id: tablet.tablet_id(),
            policy_kind: decision.policy_kind,
            cumulative_point_action: decision.cumulative_point_action,
            execution_layout,
            merge_semantics,
            input_rowsets,
            read_snapshot: ReadSnapshot {
                visible_version: tablet.max_version(),
                layout_epoch: tablet.layout_epoch(),
                schema_epoch: tablet.schema_epoch(),
            },
            output_version,
            output_rowset_id: tablet.next_rowset_id(),
            score: decision.score,
            reason: decision.reason,
            pk_delta_guard,
        }))
    }

    fn plan_primary_key(
        tablet: &Tablet,
        captured: &CompactionRowsetSet,
    ) -> Result<Option<CompactionPlan>> {
        if captured.rowsets().len() <= 1 {
            return Ok(None);
        }
        let rowsets = captured.rowsets().to_vec();

        let output_version = output_version_for(&rowsets)?;
        let input_rowsets: Vec<CompactionInput> =
            rowsets.into_iter().map(CompactionInput::new).collect();
        let score = input_rowsets.len() as f64;
        let pk_delta_guard = build_pk_delta_guard(tablet, &input_rowsets)?;

        Ok(Some(CompactionPlan {
            plan_id: next_plan_id(),
            tablet_id: tablet.tablet_id(),
            policy_kind: PolicyKind::PrimaryKeyFull,
            cumulative_point_action: CumulativePointAction::Preserve,
            execution_layout: ExecutionLayout::Horizontal,
            merge_semantics: MergeSemantics::Deduplicate,
            input_rowsets,
            read_snapshot: ReadSnapshot {
                visible_version: tablet.max_version(),
                layout_epoch: tablet.layout_epoch(),
                schema_epoch: tablet.schema_epoch(),
            },
            output_version,
            output_rowset_id: tablet.next_rowset_id(),
            score,
            reason: CompactionReason::PrimaryKeyFullDedup,
            pk_delta_guard: Some(pk_delta_guard),
        }))
    }

    pub fn plan_with_policy<P: CompactionPolicy>(
        tablet: &Tablet,
        policy: &P,
    ) -> Result<Option<CompactionPlan>> {
        let Some(schema) = tablet.schema() else {
            return Ok(None);
        };
        let rowsets = CompactionRowsetSet::capture(tablet)?;
        if schema.keys_type() == KeysType::PrimaryKeys {
            return Self::plan_primary_key(tablet, &rowsets);
        }

        let Some(CompactionSelection { decision, rowsets }) = Self::select(&rowsets, policy)?
        else {
            return Ok(None);
        };

        let merge_semantics = match schema.keys_type() {
            KeysType::DuplicateKeys => MergeSemantics::Append,
            KeysType::AggregateKeys => MergeSemantics::Aggregate,
            KeysType::PrimaryKeys => unreachable!("PRIMARY_KEYS is handled by plan_primary_key"),
            KeysType::UniqueKeys => {
                return Err(paro_error::not_supported(
                    "UNIQUE_KEYS compaction is reserved for future KeyedPublishDelta work",
                ));
            }
        };

        let output_version = output_version_for(&rowsets)?;
        let input_rowsets: Vec<CompactionInput> =
            rowsets.into_iter().map(CompactionInput::new).collect();
        let execution_layout = if schema.columns().len() > 10 {
            ExecutionLayout::Vertical
        } else {
            ExecutionLayout::Horizontal
        };

        Ok(Some(CompactionPlan {
            plan_id: next_plan_id(),
            tablet_id: tablet.tablet_id(),
            policy_kind: decision.policy_kind,
            cumulative_point_action: decision.cumulative_point_action,
            execution_layout,
            merge_semantics,
            input_rowsets,
            read_snapshot: ReadSnapshot {
                visible_version: tablet.max_version(),
                layout_epoch: tablet.layout_epoch(),
                schema_epoch: tablet.schema_epoch(),
            },
            output_version,
            output_rowset_id: tablet.next_rowset_id(),
            score: decision.score,
            reason: decision.reason,
            pk_delta_guard: None,
        }))
    }

    fn select<P: CompactionPolicy + ?Sized>(
        rowsets: &CompactionRowsetSet,
        policy: &P,
    ) -> Result<Option<CompactionSelection>> {
        let Some(selection) = policy.select(rowsets)? else {
            return Ok(None);
        };
        if selection.rowsets.is_empty() {
            return Ok(None);
        }
        if !rewrite_amplification_admitted(&selection.rowsets) {
            return Ok(None);
        }
        Ok(Some(selection))
    }
}

fn output_version_for(rowsets: &[RowsetSharedPtr]) -> Result<Version> {
    let first = rowsets
        .first()
        .ok_or_else(|| paro_error::internal("compaction rowsets must not be empty"))?;
    let last = rowsets
        .last()
        .ok_or_else(|| paro_error::internal("compaction rowsets must not be empty"))?;
    Ok(Version::new(first.start_version(), last.end_version()))
}

fn build_pk_delta_guard(
    tablet: &Tablet,
    input_rowsets: &[CompactionInput],
) -> Result<PkDeltaGuard> {
    let guard = PkDeltaGuard {
        estimated_rows: input_rowsets.iter().map(|input| input.num_rows).sum(),
        estimated_bytes: input_rowsets.iter().map(|input| input.size_bytes).sum(),
        max_rows: PK_PUBLISH_DELTA_MAX_ROWS,
        max_bytes: PK_PUBLISH_DELTA_MAX_BYTES,
    };
    if !guard.within_limits() {
        return Err(paro_error::configuration_limit_exceeded(format!(
            "planned PK compaction for tablet {} exceeds publish-delta guard (rows={} bytes={})",
            tablet.tablet_id(),
            guard.estimated_rows,
            guard.estimated_bytes
        )));
    }
    Ok(guard)
}

fn next_plan_id() -> CompactionPlanId {
    static NEXT_PLAN_ID: AtomicU64 = AtomicU64::new(1);
    CompactionPlanId(NEXT_PLAN_ID.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use paro_common::types::LogicalType;

    use super::*;
    use crate::rowset::{Rowset, RowsetMeta};
    use crate::tablet::{TabletColumn, TabletSchema};

    fn test_tablet() -> (tempfile::TempDir, Tablet) {
        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(
            TabletSchema::new(
                1,
                vec![TabletColumn::key(0, "id", LogicalType::BigInt)],
                KeysType::DuplicateKeys,
            )
            .unwrap(),
        );
        let tablet = Tablet::new(1, 1, 0, schema, temp.path(), None).unwrap();
        (temp, tablet)
    }

    fn add_sized_rowset(
        tablet: &Tablet,
        id: u64,
        version: i64,
        bytes: u64,
        compaction_output: bool,
    ) {
        let mut meta = RowsetMeta::new(id, tablet.tablet_id(), Version::singleton(version));
        meta.set_disk_sizes(bytes, 0);
        if compaction_output {
            meta.set_compaction_output(vec![id + 100]);
        }
        let rowset = Rowset::create(
            tablet.schema().expect("test tablet schema"),
            meta,
            tablet.data_dir().join(format!("rowset-{id}")),
        )
        .unwrap();
        tablet.add_rowset(Arc::new(rowset)).unwrap();
    }

    #[test]
    fn planner_compacts_tiny_deltas_without_rewriting_large_base() {
        let (_temp, tablet) = test_tablet();
        add_sized_rowset(&tablet, 1, 1, 100 * 1024 * 1024 * 1024, true);
        add_sized_rowset(&tablet, 2, 2, 4 * 1024 * 1024, false);
        add_sized_rowset(&tablet, 3, 3, 4 * 1024 * 1024, false);
        tablet.set_cumulative_point(2);

        let plan = CompactionPlanner::plan(&tablet).unwrap().unwrap();
        assert_eq!(plan.policy_kind, PolicyKind::SizeTiered);
        assert_eq!(
            plan.input_rowsets
                .iter()
                .map(|input| input.rowset.rowset_id())
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(plan.planned_input_size(), 8 * 1024 * 1024);
    }

    #[test]
    fn shared_gate_bounds_base_rewrite_amplification() {
        let (_temp, tablet) = test_tablet();
        add_sized_rowset(&tablet, 1, 1, 100 * 1024 * 1024 * 1024, true);
        add_sized_rowset(&tablet, 2, 2, 4 * 1024 * 1024, false);
        add_sized_rowset(&tablet, 3, 3, 4 * 1024 * 1024, false);
        tablet.set_cumulative_point(4);

        assert!(
            CompactionPlanner::plan_with_policy(&tablet, &BaseCompactionPolicy::new())
                .unwrap()
                .is_none()
        );

        add_sized_rowset(&tablet, 4, 4, 20 * 1024 * 1024 * 1024, false);
        tablet.set_cumulative_point(5);
        let plan = CompactionPlanner::plan_with_policy(&tablet, &BaseCompactionPolicy::new())
            .unwrap()
            .expect("one newer byte per five baseline bytes admits the rewrite");
        assert_eq!(plan.input_rowsets.len(), 4);
    }
}
