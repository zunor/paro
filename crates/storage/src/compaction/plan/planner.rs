// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::plan::policy::{
    BaseCompactionPolicy, CompactionPolicy, CompactionRowsetSet, CompactionSelection,
    CumulativeCompactionPolicy, SizeTieredCompactionPolicy,
};
#[cfg(test)]
use crate::compaction::plan::types::PolicyKind;
use crate::compaction::plan::types::{
    CompactionGoal, CompactionInput, CompactionPlan, CompactionPlanId, CompactionReason,
    CumulativePointAction, ExecutionLayout, MergeSemantics, PkDeltaGuard, PrimaryIndexPublishPlan,
    ReadSnapshot,
};
use crate::compaction::publish::{PkIndexUpsertCandidate, SegmentDeleteDelta};
use crate::rowset::RowsetSharedPtr;
use crate::tablet::{KeysType, Tablet, Version};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use std::collections::BTreeSet;
use std::mem::size_of;
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
        Self::plan_for_goal(tablet, CompactionGoal::ReduceDebt)
    }

    pub fn plan_for_goal(tablet: &Tablet, goal: CompactionGoal) -> Result<Option<CompactionPlan>> {
        let Some(schema) = tablet.schema() else {
            return Ok(None);
        };
        let rowsets = CompactionRowsetSet::capture(tablet)?;

        if let CompactionGoal::CoalesceTo { max_rowsets } = goal {
            return Self::plan_coalesce(tablet, &rowsets, max_rowsets.max(1));
        }

        if schema.keys_type() == KeysType::PrimaryKeys {
            return Self::plan_primary_key(tablet, &rowsets, goal);
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
            goal,
            primary_index_publish: None,
        }))
    }

    fn plan_primary_key(
        tablet: &Tablet,
        captured: &CompactionRowsetSet,
        goal: CompactionGoal,
    ) -> Result<Option<CompactionPlan>> {
        if captured.rowsets().len() <= 1 {
            return Ok(None);
        }

        // Primary-key semantics belong to the merger, not to a full-table
        // selection policy. Rewriting an established base for every new
        // rowset creates unbounded write/index amplification and can starve
        // HNSW tail catch-up. Select the same leveled, contiguous ranges as
        // other tables and apply latest-key deduplication within that range.
        let size_tiered = SizeTieredCompactionPolicy::new();
        let cumulative = CumulativeCompactionPolicy::new();
        let base = BaseCompactionPolicy::new();
        let policies: [&dyn CompactionPolicy; 3] = [&size_tiered, &cumulative, &base];
        let mut selected = None;
        for policy in policies {
            if let Some(selection) = Self::select(captured, policy)? {
                selected = Some(selection);
                break;
            }
        }
        let Some(CompactionSelection {
            decision,
            rowsets: selected_rowsets,
        }) = selected
        else {
            return Ok(None);
        };

        let (rowsets, primary_index_publish, cumulative_point_action) =
            match bounded_pk_selection(tablet, &selected_rowsets)? {
                Some((start, rowsets, guard)) => {
                    let action = if start == 0 {
                        decision.cumulative_point_action
                    } else {
                        // A later bounded window must not advance the durable
                        // cumulative point past untouched earlier deltas.
                        CumulativePointAction::Preserve
                    };
                    (rowsets, PrimaryIndexPublishPlan::Incremental(guard), action)
                }
                None => (
                    selected_rowsets,
                    PrimaryIndexPublishPlan::RebuildFromVisibleRowsets,
                    decision.cumulative_point_action,
                ),
            };

        let output_version = output_version_for(&rowsets)?;
        let input_rowsets: Vec<CompactionInput> =
            rowsets.into_iter().map(CompactionInput::new).collect();

        Ok(Some(CompactionPlan {
            plan_id: next_plan_id(),
            tablet_id: tablet.tablet_id(),
            policy_kind: decision.policy_kind,
            cumulative_point_action,
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
            score: decision.score,
            reason: decision.reason,
            goal,
            primary_index_publish: Some(primary_index_publish),
        }))
    }

    fn plan_coalesce(
        tablet: &Tablet,
        captured: &CompactionRowsetSet,
        max_rowsets: usize,
    ) -> Result<Option<CompactionPlan>> {
        let schema = tablet
            .schema()
            .ok_or_else(|| paro_error::internal("coalesce planning requires tablet schema"))?;
        if captured.rowsets().len() <= max_rowsets {
            return Ok(None);
        }
        // One output replaces enough of the oldest contiguous prefix to meet
        // the requested physical target. Replanning after every publication
        // makes this target verifiable even when PK delta bounds split work.
        let merge_count = captured
            .rowsets()
            .len()
            .saturating_sub(max_rowsets)
            .saturating_add(1)
            .max(2);
        let requested = captured.rowsets()[..merge_count].to_vec();
        let merge_semantics = match schema.keys_type() {
            KeysType::DuplicateKeys => MergeSemantics::Append,
            KeysType::AggregateKeys => MergeSemantics::Aggregate,
            KeysType::PrimaryKeys => MergeSemantics::Deduplicate,
            KeysType::UniqueKeys => MergeSemantics::UniqueLatest,
        };
        let (rowsets, primary_index_publish) = if merge_semantics == MergeSemantics::Deduplicate {
            match bounded_pk_selection(tablet, &requested)? {
                Some((_start, rowsets, guard)) => {
                    (rowsets, Some(PrimaryIndexPublishPlan::Incremental(guard)))
                }
                None => (
                    requested,
                    Some(PrimaryIndexPublishPlan::RebuildFromVisibleRowsets),
                ),
            }
        } else {
            (requested, None)
        };
        let output_version = output_version_for(&rowsets)?;
        let crosses_cumulative = rowsets
            .last()
            .is_some_and(|rowset| rowset.end_version() >= captured.cumulative_point());
        let input_rowsets = rowsets.into_iter().map(CompactionInput::new).collect();
        Ok(Some(CompactionPlan {
            plan_id: next_plan_id(),
            tablet_id: tablet.tablet_id(),
            policy_kind: crate::compaction::plan::types::PolicyKind::Goal,
            cumulative_point_action: if crosses_cumulative {
                CumulativePointAction::AdvanceToOutputEndExclusive
            } else {
                CumulativePointAction::Preserve
            },
            execution_layout: if merge_semantics == MergeSemantics::Deduplicate
                || schema.columns().len() <= 10
            {
                ExecutionLayout::Horizontal
            } else {
                ExecutionLayout::Vertical
            },
            merge_semantics,
            input_rowsets,
            read_snapshot: ReadSnapshot {
                visible_version: tablet.max_version(),
                layout_epoch: tablet.layout_epoch(),
                schema_epoch: tablet.schema_epoch(),
            },
            output_version,
            output_rowset_id: tablet.next_rowset_id(),
            score: f64::INFINITY,
            reason: CompactionReason::ExplicitCoalesce,
            goal: CompactionGoal::CoalesceTo { max_rowsets },
            primary_index_publish,
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
            return Self::plan_primary_key(tablet, &rowsets, CompactionGoal::ReduceDebt);
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
            goal: CompactionGoal::ReduceDebt,
            primary_index_publish: None,
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

fn estimate_pk_delta_guard(
    tablet: &Tablet,
    input_rowsets: &[CompactionInput],
) -> Result<PkDeltaGuard> {
    let estimated_rows = input_rowsets.iter().map(|input| input.num_rows).sum();
    let guard = PkDeltaGuard {
        estimated_rows,
        estimated_bytes: estimate_pk_publish_delta_bytes(tablet, input_rowsets, estimated_rows)?,
        max_rows: PK_PUBLISH_DELTA_MAX_ROWS,
        max_bytes: PK_PUBLISH_DELTA_MAX_BYTES,
    };
    Ok(guard)
}

/// Select the largest row-count contiguous window whose ephemeral primary
/// index delta fits the publication envelope. `None` means no rowset-level
/// window of at least two inputs can fit and the planner must use the durable
/// primary-index rebuild strategy instead of stalling forever.
fn bounded_pk_selection(
    tablet: &Tablet,
    rowsets: &[RowsetSharedPtr],
) -> Result<Option<(usize, Vec<RowsetSharedPtr>, PkDeltaGuard)>> {
    if rowsets.len() < 2 {
        return Ok(None);
    }
    let inputs = rowsets
        .iter()
        .cloned()
        .map(CompactionInput::new)
        .collect::<Vec<_>>();
    let full_guard = estimate_pk_delta_guard(tablet, &inputs)?;
    if full_guard.within_limits() {
        return Ok(Some((0, rowsets.to_vec(), full_guard)));
    }

    let footprints = inputs
        .iter()
        .map(|input| estimate_pk_delta_guard(tablet, std::slice::from_ref(input)))
        .collect::<Result<Vec<_>>>()?;
    let mut left = 0usize;
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut best: Option<(usize, usize, u64)> = None;
    for right in 0..footprints.len() {
        rows = rows.saturating_add(footprints[right].estimated_rows);
        bytes = bytes.saturating_add(footprints[right].estimated_bytes);
        while left <= right
            && (rows > PK_PUBLISH_DELTA_MAX_ROWS || bytes > PK_PUBLISH_DELTA_MAX_BYTES)
        {
            rows = rows.saturating_sub(footprints[left].estimated_rows);
            bytes = bytes.saturating_sub(footprints[left].estimated_bytes);
            left += 1;
        }
        let len = right.saturating_sub(left).saturating_add(1);
        if len >= 2
            && best.as_ref().is_none_or(|(_, best_len, best_rows)| {
                len > *best_len || (len == *best_len && rows > *best_rows)
            })
        {
            best = Some((left, len, rows));
        }
    }
    let Some((start, len, _)) = best else {
        return Ok(None);
    };
    let selected = rowsets[start..start + len].to_vec();
    let selected_inputs = selected
        .iter()
        .cloned()
        .map(CompactionInput::new)
        .collect::<Vec<_>>();
    let guard = estimate_pk_delta_guard(tablet, &selected_inputs)?;
    debug_assert!(guard.within_limits());
    Ok(Some((start, selected, guard)))
}

/// Estimate the retained publication delta, not the table rows being rewritten.
///
/// A PK compaction row may contain a multi-kilobyte vector while its publish
/// record contains only the encoded primary key and two physical locations.
/// Charging the full row payload makes the resource guard dimension-dependent
/// and rejects perfectly bounded compactions. Fixed-width and bounded keys are
/// derived from the durable schema. For unbounded byte keys, segment column
/// footprints provide a data-dependent estimate without charging unrelated
/// value/vector columns.
fn estimate_pk_publish_delta_bytes(
    tablet: &Tablet,
    input_rowsets: &[CompactionInput],
    estimated_rows: u64,
) -> Result<u64> {
    let schema = tablet
        .schema()
        .ok_or_else(|| paro_error::internal("PK delta estimate requires tablet schema"))?;
    let key_columns = schema.key_columns();
    let fixed_or_bounded_key_bytes = key_columns.iter().try_fold(0_u64, |total, column| {
        encoded_key_column_bound(&column.logical_type, column.length)
            .map(|bytes| total.saturating_add(bytes))
    });
    let key_payload_bytes = match fixed_or_bounded_key_bytes {
        Some(per_row) => estimated_rows.saturating_mul(per_row),
        None => estimate_unbounded_key_payload(input_rowsets, key_columns)?,
    };

    let candidate_headers = estimated_rows
        .saturating_mul(u64::try_from(size_of::<PkIndexUpsertCandidate>()).unwrap_or(u64::MAX));
    let segment_count = input_rowsets.iter().try_fold(0_u64, |total, input| {
        input.rowset.load()?;
        Ok::<_, paro_common::error::ParoError>(
            total.saturating_add(u64::from(input.rowset.num_segments())),
        )
    })?;
    let delete_delta_headers = segment_count
        .saturating_mul(u64::try_from(size_of::<SegmentDeleteDelta>()).unwrap_or(u64::MAX));
    // Internal duplicate deletion is represented as a bitmap. One bit per
    // input row is the conservative dense bound; sparse containers are no
    // larger for the cardinalities where they are selected.
    let delete_bitmap_bytes = estimated_rows.div_ceil(8);

    Ok(candidate_headers
        .saturating_add(key_payload_bytes)
        .saturating_add(delete_delta_headers)
        .saturating_add(delete_bitmap_bytes))
}

fn encoded_key_column_bound(logical_type: &LogicalType, declared_length: u32) -> Option<u64> {
    let fixed = match logical_type {
        LogicalType::Boolean | LogicalType::TinyInt | LogicalType::UTinyInt => 1,
        LogicalType::SmallInt | LogicalType::USmallInt => 2,
        LogicalType::Integer | LogicalType::UInteger | LogicalType::Float | LogicalType::Date => 4,
        LogicalType::BigInt
        | LogicalType::UBigInt
        | LogicalType::Double
        | LogicalType::Timestamp
        | LogicalType::TimestampTz
        | LogicalType::Time => 8,
        LogicalType::HugeInt | LogicalType::UHugeInt | LogicalType::Uuid => 16,
        LogicalType::Decimal { precision, .. } => {
            if *precision <= 18 {
                8
            } else {
                16
            }
        }
        LogicalType::Varchar
        | LogicalType::VarcharCollation(_)
        | LogicalType::TsVector
        | LogicalType::TsQuery
        | LogicalType::Blob
        | LogicalType::Json
        | LogicalType::Jsonb => {
            if declared_length == 0 {
                return None;
            }
            // Comparable byte encoding stores an 8-byte group plus marker and
            // an additional terminator group for exact multiples of eight.
            return Some((u64::from(declared_length) / 8 + 1).saturating_mul(9));
        }
        _ => return None,
    };
    Some(fixed)
}

fn estimate_unbounded_key_payload(
    input_rowsets: &[CompactionInput],
    key_columns: &[crate::tablet::TabletColumn],
) -> Result<u64> {
    let key_ids = key_columns
        .iter()
        .map(|column| column.id)
        .collect::<BTreeSet<_>>();
    let mut raw_bytes = 0_u64;
    let mut rows = 0_u64;
    for input in input_rowsets {
        input.rowset.load()?;
        rows = rows.saturating_add(input.num_rows);
        for segment in input.rowset.segments() {
            raw_bytes = raw_bytes.saturating_add(
                segment
                    .column_metas()
                    .iter()
                    .filter(|meta| key_ids.contains(&meta.column_id))
                    .map(|meta| meta.total_mem_footprint)
                    .sum::<u64>(),
            );
        }
    }
    // Account for 8-byte comparable groups and one terminator group per key
    // row. The source footprint may already contain offsets/null metadata, so
    // this intentionally remains conservative.
    Ok(raw_bytes
        .saturating_add(raw_bytes.div_ceil(8))
        .saturating_add(rows.saturating_mul(9)))
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

    fn test_primary_key_vector_tablet(dimension: usize) -> (tempfile::TempDir, Tablet) {
        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(
            TabletSchema::new(
                2,
                vec![
                    TabletColumn::key(0, "id", LogicalType::BigInt),
                    TabletColumn::new(
                        1,
                        "embedding",
                        LogicalType::Array(Box::new(LogicalType::Float), dimension),
                    ),
                ],
                KeysType::PrimaryKeys,
            )
            .unwrap(),
        );
        let tablet = Tablet::new(2, 1, 0, schema, temp.path(), None).unwrap();
        (temp, tablet)
    }

    fn add_sized_rowset(
        tablet: &Tablet,
        id: u64,
        version: i64,
        bytes: u64,
        compaction_output: bool,
    ) {
        add_sized_rowset_with_rows(tablet, id, version, bytes, 0, compaction_output);
    }

    fn add_sized_rowset_with_rows(
        tablet: &Tablet,
        id: u64,
        version: i64,
        bytes: u64,
        rows: u64,
        compaction_output: bool,
    ) {
        let mut meta = RowsetMeta::new(id, tablet.tablet_id(), Version::singleton(version));
        meta.set_disk_sizes(bytes, 0);
        meta.set_num_rows(rows);
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

    #[test]
    fn pk_publish_delta_estimate_excludes_non_key_vector_payload() {
        let (_temp, tablet) = test_primary_key_vector_tablet(768);
        let rows = 1_000_000;
        let estimated = estimate_pk_publish_delta_bytes(&tablet, &[], rows).unwrap();

        assert!(estimated < PK_PUBLISH_DELTA_MAX_BYTES);
        assert_eq!(
            estimated,
            rows * u64::try_from(size_of::<PkIndexUpsertCandidate>()).unwrap()
                + rows * size_of::<i64>() as u64
                + rows.div_ceil(8)
        );
    }

    #[test]
    fn comparable_key_width_models_fixed_and_bounded_values() {
        assert_eq!(encoded_key_column_bound(&LogicalType::BigInt, 0), Some(8));
        assert_eq!(encoded_key_column_bound(&LogicalType::Uuid, 0), Some(16));
        assert_eq!(encoded_key_column_bound(&LogicalType::Varchar, 8), Some(18));
        assert_eq!(encoded_key_column_bound(&LogicalType::Varchar, 9), Some(18));
        assert_eq!(encoded_key_column_bound(&LogicalType::Varchar, 0), None);
    }

    #[test]
    fn primary_key_planner_compacts_delta_level_without_rewriting_large_base() {
        let (_temp, tablet) = test_primary_key_vector_tablet(128);
        add_sized_rowset(&tablet, 1, 1, 512 * 1024 * 1024, true);
        add_sized_rowset(&tablet, 2, 2, 4 * 1024 * 1024, false);
        add_sized_rowset(&tablet, 3, 3, 4 * 1024 * 1024, false);

        let plan = CompactionPlanner::plan(&tablet).unwrap().unwrap();
        assert_eq!(plan.merge_semantics, MergeSemantics::Deduplicate);
        assert_eq!(plan.policy_kind, PolicyKind::SizeTiered);
        assert_eq!(
            plan.input_rowsets
                .iter()
                .map(|input| input.rowset.rowset_id())
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn primary_key_planner_defers_tiny_delta_against_large_base() {
        let (_temp, tablet) = test_primary_key_vector_tablet(128);
        add_sized_rowset(&tablet, 1, 1, 512 * 1024 * 1024, true);
        add_sized_rowset(&tablet, 2, 2, 4 * 1024 * 1024, false);

        assert!(CompactionPlanner::plan(&tablet).unwrap().is_none());
    }

    #[test]
    fn oversized_pk_delta_uses_durable_rebuild_instead_of_stalling() {
        let (_temp, tablet) = test_primary_key_vector_tablet(128);
        add_sized_rowset_with_rows(&tablet, 1, 1, 256 * 1024 * 1024, 3_000_000, false);
        add_sized_rowset_with_rows(&tablet, 2, 2, 256 * 1024 * 1024, 3_000_000, false);

        let plan = CompactionPlanner::plan(&tablet)
            .unwrap()
            .expect("two oversized PK rowsets must still make progress");
        assert_eq!(plan.planned_input_rows(), 6_000_000);
        assert_eq!(
            plan.primary_index_publish,
            Some(PrimaryIndexPublishPlan::RebuildFromVisibleRowsets)
        );
    }

    #[test]
    fn explicit_coalesce_bypasses_background_write_amplification_gate() {
        let (_temp, tablet) = test_tablet();
        add_sized_rowset(&tablet, 1, 1, 95 * 1024 * 1024, true);
        add_sized_rowset(&tablet, 2, 2, 5 * 1024 * 1024, false);

        assert!(CompactionPlanner::plan(&tablet).unwrap().is_none());
        let plan = CompactionPlanner::plan_for_goal(
            &tablet,
            CompactionGoal::CoalesceTo { max_rowsets: 1 },
        )
        .unwrap()
        .expect("explicit physical target must not reuse background heuristics");
        assert_eq!(plan.input_rowsets.len(), 2);
        assert_eq!(plan.policy_kind, PolicyKind::Goal);
        assert_eq!(plan.reason, CompactionReason::ExplicitCoalesce);
    }
}
