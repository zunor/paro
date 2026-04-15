use crate::rowset::{RowsetId, RowsetSharedPtr};
use crate::tablet::Version;
use std::fmt;

/// Logical identity of "what should be compacted".
///
/// A single plan can be retried multiple times, so a publish conflict or
/// execution failure must not force a new semantic plan id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CompactionPlanId(pub u64);

impl fmt::Display for CompactionPlanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "plan_{}", self.0)
    }
}

/// Physical identity of one execution attempt for a compaction plan.
///
/// Retries allocate a fresh job id so staging directories and lifecycle
/// tracking never alias an earlier failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CompactionJobId(pub u64);

impl fmt::Display for CompactionJobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "job_{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PolicyKind {
    Base,
    Cumulative,
    SizeTiered,
    PrimaryKeyFull,
}

impl fmt::Display for PolicyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolicyKind::Base => write!(f, "BASE"),
            PolicyKind::Cumulative => write!(f, "CUMULATIVE"),
            PolicyKind::SizeTiered => write!(f, "SIZE_TIERED"),
            PolicyKind::PrimaryKeyFull => write!(f, "PRIMARY_KEY_FULL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CumulativePointAction {
    Preserve,
    AdvanceToOutputEndExclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionLayout {
    Horizontal,
    Vertical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeSemantics {
    Append,
    Aggregate,
    UniqueLatest,
    Deduplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactionReason {
    BasePolicy,
    CumulativePolicy,
    SizeTieredPolicy,
    PrimaryKeyFullDedup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactionLifecycleState {
    Planned,
    Building,
    Validated,
    Publishing,
    RetiredPendingGc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadSnapshot {
    pub visible_version: i64,
    pub schema_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CompactionInput {
    pub rowset: RowsetSharedPtr,
    pub num_rows: u64,
    pub size_bytes: u64,
}

impl CompactionInput {
    pub fn new(rowset: RowsetSharedPtr) -> Self {
        Self {
            num_rows: rowset.num_rows(),
            size_bytes: rowset.total_disk_size(),
            rowset,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PkDeltaGuard {
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    pub max_rows: u64,
    pub max_bytes: u64,
}

impl PkDeltaGuard {
    pub fn within_limits(&self) -> bool {
        self.estimated_rows <= self.max_rows && self.estimated_bytes <= self.max_bytes
    }
}

#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub plan_id: CompactionPlanId,
    pub tablet_id: u64,
    pub policy_kind: PolicyKind,
    pub cumulative_point_action: CumulativePointAction,
    pub execution_layout: ExecutionLayout,
    pub merge_semantics: MergeSemantics,
    pub input_rowsets: Vec<CompactionInput>,
    pub read_snapshot: ReadSnapshot,
    pub output_version: Version,
    pub output_rowset_id: RowsetId,
    pub score: f64,
    pub reason: CompactionReason,
    pub pk_delta_guard: Option<PkDeltaGuard>,
}

impl CompactionPlan {
    pub fn planned_input_rows(&self) -> u64 {
        self.input_rowsets.iter().map(|input| input.num_rows).sum()
    }

    pub fn planned_input_size(&self) -> u64 {
        self.input_rowsets
            .iter()
            .map(|input| input.size_bytes)
            .sum()
    }

    pub fn input_rowset_ptrs(&self) -> Vec<RowsetSharedPtr> {
        self.input_rowsets
            .iter()
            .map(|input| input.rowset.clone())
            .collect()
    }
}
