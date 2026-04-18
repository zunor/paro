// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::compaction::plan::policy::{
    BaseCompactionPolicy, CompactionDecision, CompactionPolicy, CumulativeCompactionPolicy,
    SizeTieredCompactionPolicy,
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

pub struct CompactionPlanner;

impl CompactionPlanner {
    pub fn plan(tablet: &Tablet) -> Result<Option<CompactionPlan>> {
        let Some(schema) = tablet.schema() else {
            return Ok(None);
        };

        if schema.keys_type() == KeysType::PrimaryKeys {
            return Self::plan_primary_key(tablet);
        }

        let decision_and_inputs = [
            Self::select(tablet, &SizeTieredCompactionPolicy::new())?,
            Self::select(tablet, &CumulativeCompactionPolicy::new())?,
            Self::select(tablet, &BaseCompactionPolicy::new())?,
        ]
        .into_iter()
        .flatten()
        .next();

        let Some((decision, rowsets)) = decision_and_inputs else {
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
                rowset_epoch: tablet.rowset_epoch(),
                schema_epoch: tablet.schema_epoch(),
            },
            output_version,
            output_rowset_id: tablet.next_rowset_id(),
            score: decision.score,
            reason: decision.reason,
            pk_delta_guard,
        }))
    }

    fn plan_primary_key(tablet: &Tablet) -> Result<Option<CompactionPlan>> {
        let mut rowsets = tablet.capture_consistent_rowsets(tablet.max_version())?;
        if rowsets.len() <= 1 {
            return Ok(None);
        }
        rowsets.sort_by_key(|rowset| rowset.start_version());

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
                rowset_epoch: tablet.rowset_epoch(),
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
        if schema.keys_type() == KeysType::PrimaryKeys {
            return Self::plan_primary_key(tablet);
        }

        let Some((decision, rowsets)) = Self::select(tablet, policy)? else {
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
                rowset_epoch: tablet.rowset_epoch(),
                schema_epoch: tablet.schema_epoch(),
            },
            output_version,
            output_rowset_id: tablet.next_rowset_id(),
            score: decision.score,
            reason: decision.reason,
            pk_delta_guard: None,
        }))
    }

    fn select<P: CompactionPolicy>(
        tablet: &Tablet,
        policy: &P,
    ) -> Result<Option<(CompactionDecision, Vec<crate::rowset::RowsetSharedPtr>)>> {
        let Some(decision) = policy.select(tablet)? else {
            return Ok(None);
        };
        let rowsets = policy.pick_rowsets(tablet)?;
        if rowsets.is_empty() {
            return Ok(None);
        }

        if decision.policy_kind == PolicyKind::SizeTiered
            && rowsets
                .first()
                .map(|rowset| rowset.start_version())
                .unwrap_or_default()
                == 0
            && rowsets
                .iter()
                .any(|rowset| rowset.start_version() >= tablet.cumulative_point())
        {
            return Ok(None);
        }

        Ok(Some((decision, rowsets)))
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
