// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stable rule and implementation registries used by Direct and Memo search.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use paro_common::error::{self as paro_error, Result};
use paro_planner::expression::Expression;

use super::budget::BudgetDimension;
use super::calibration::ParallelWorkProfile;
use super::cost::SearchCost;
use super::ids::{
    Fingerprint, GroupId, ImplementationId, LogicalExprId, LogicalPayloadId, PhysicalPayloadId,
    RuleId, StableFingerprintBuilder,
};
use super::memo::{
    EquivalenceProof, FrozenCandidate, LogicalExpr, LogicalExprKey, Memo, OptimizationGoal,
    PhysicalExprKey,
};
use super::properties::ProvidedProperties;
use super::region::RegionCandidateContract;

pub const CTE_INLINE_RULE: RuleId = RuleId(10_007);
pub const CTE_DEMAND_PUSHDOWN_RULE: RuleId = RuleId(10_008);
pub const AGGREGATE_POST_REDUCTION_RULE: RuleId = RuleId(10_009);
pub const MARK_JOIN_TO_SEMI_RULE: RuleId = RuleId(10_010);
pub const JOIN_ELIMINATION_RULE: RuleId = RuleId(10_011);
pub const AGGREGATE_JOIN_PREAGGREGATION_RULE: RuleId = RuleId(10_012);
pub const AGGREGATE_JOIN_SUBSUMPTION_RULE: RuleId = RuleId(10_013);
pub const AGGREGATE_NON_NULL_INPUT_RULE: RuleId = RuleId(10_014);
pub const AGGREGATE_DIMENSION_DEFERRAL_RULE: RuleId = RuleId(10_015);
pub const AGGREGATE_INPUT_MATERIALIZATION_RULE: RuleId = RuleId(10_016);
pub const LIMIT_PUSHDOWN_RULE: RuleId = RuleId(10_017);
pub const LATE_PAYLOAD_FETCH_RULE: RuleId = RuleId(10_018);
pub const SCALAR_AGGREGATE_WINDOW_RULE: RuleId = RuleId(10_019);
pub const JOIN_REGION_ENUMERATION_RULE: RuleId = RuleId(10_021);
pub const TOP_N_INTRODUCTION_RULE: RuleId = RuleId(10_022);
pub const CTE_FILTER_PUSHDOWN_RULE: RuleId = RuleId(10_023);
pub const CTE_PARTITIONED_MATERIALIZATION_RULE: RuleId = RuleId(10_024);
pub const AGGREGATE_DIMENSION_SHARING_RULE: RuleId = RuleId(10_025);
pub const PREDICATE_TRANSFER_RULE: RuleId = RuleId(10_026);
pub const KEY_DOMAIN_TRANSFER_RULE: RuleId = RuleId(10_027);

const TRANSFORMATION_RULE_NAMES: &[(RuleId, &str)] = &[
    (CTE_INLINE_RULE, "cte_inline"),
    (CTE_DEMAND_PUSHDOWN_RULE, "cte_demand_pushdown"),
    (CTE_FILTER_PUSHDOWN_RULE, "cte_filter_pushdown"),
    (
        CTE_PARTITIONED_MATERIALIZATION_RULE,
        "cte_partitioned_materialization",
    ),
    (AGGREGATE_POST_REDUCTION_RULE, "aggregate_post_reduction"),
    (MARK_JOIN_TO_SEMI_RULE, "mark_join_to_semi"),
    (JOIN_ELIMINATION_RULE, "join_elimination"),
    (
        AGGREGATE_JOIN_PREAGGREGATION_RULE,
        "aggregate_join_preaggregation",
    ),
    (
        AGGREGATE_JOIN_SUBSUMPTION_RULE,
        "aggregate_join_subsumption",
    ),
    (AGGREGATE_NON_NULL_INPUT_RULE, "aggregate_non_null_input"),
    (
        AGGREGATE_DIMENSION_DEFERRAL_RULE,
        "aggregate_dimension_deferral",
    ),
    (
        AGGREGATE_DIMENSION_SHARING_RULE,
        "aggregate_dimension_sharing",
    ),
    (
        AGGREGATE_INPUT_MATERIALIZATION_RULE,
        "aggregate_input_materialization",
    ),
    (LIMIT_PUSHDOWN_RULE, "limit_pushdown"),
    (LATE_PAYLOAD_FETCH_RULE, "late_payload_fetch"),
    (SCALAR_AGGREGATE_WINDOW_RULE, "scalar_aggregate_window"),
    (JOIN_REGION_ENUMERATION_RULE, "join_region_enumeration"),
    (TOP_N_INTRODUCTION_RULE, "top_n_introduction"),
    (PREDICATE_TRANSFER_RULE, "predicate_transfer"),
    (KEY_DOMAIN_TRANSFER_RULE, "key_domain_transfer"),
];

pub fn transformation_rule_name(id: RuleId) -> Option<&'static str> {
    TRANSFORMATION_RULE_NAMES
        .iter()
        .find_map(|(candidate, name)| (*candidate == id).then_some(*name))
}

pub fn transformation_rule_id(name: &str) -> Option<RuleId> {
    TRANSFORMATION_RULE_NAMES
        .iter()
        .find_map(|(id, candidate)| candidate.eq_ignore_ascii_case(name.trim()).then_some(*id))
}

pub fn validate_transformation_rule_names(names: &str) -> Result<()> {
    for name in names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        if transformation_rule_id(name).is_none() {
            return Err(paro_error::invalid_input(format!(
                "unknown optimizer transformation rule '{name}'"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RulePromise {
    pub priority: u16,
}

/// Semantic dependency lane used by the quality-first scheduler.
///
/// This is deliberately separate from `RulePromise::priority`: a promise is
/// a local rule preference, while this lane describes which producer/consumer
/// contract must be published before a later quality obligation can be
/// usefully composed.  The engine only gives these lanes precedence for an
/// explicitly enabled quality handoff; ordinary searches retain the existing
/// promise order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityDependency {
    ConsumerDemand,
    DomainRestriction,
    ProducerDomain,
    NarrowAggregate,
    DimensionMerge,
    JoinSelection,
}

impl QualityDependency {
    pub const fn stage(self) -> u8 {
        match self {
            Self::ConsumerDemand => 0,
            Self::DomainRestriction => 1,
            Self::ProducerDomain => 2,
            Self::NarrowAggregate => 3,
            Self::DimensionMerge => 4,
            Self::JoinSelection => 5,
        }
    }
}

/// Resource class for optional logical search. A composition rule combines
/// already-published child alternatives and therefore needs a small reserved
/// path through every admission gate; reserving only its enumeration work or
/// child groups still lets unrelated local rewrites strand the parent result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformationBudgetClass {
    Local,
    Composition,
}

impl TransformationBudgetClass {
    pub const fn group_dimension(self) -> BudgetDimension {
        match self {
            Self::Local => BudgetDimension::Group,
            Self::Composition => BudgetDimension::CompositionGroup,
        }
    }

    pub const fn output_dimension(self) -> BudgetDimension {
        match self {
            Self::Local => BudgetDimension::LogicalExprPerGroup,
            Self::Composition => BudgetDimension::CompositionLogicalExprPerGroup,
        }
    }

    pub const fn fire_dimension(self) -> BudgetDimension {
        match self {
            Self::Local => BudgetDimension::RuleFirePerGroup,
            Self::Composition => BudgetDimension::CompositionRuleFirePerGroup,
        }
    }

    pub const fn work_dimension(self) -> BudgetDimension {
        match self {
            Self::Local => BudgetDimension::RuleWorkPerGroup,
            Self::Composition => BudgetDimension::CompositionRuleWorkPerGroup,
        }
    }
}

impl RulePromise {
    pub const HIGH: Self = Self { priority: 100 };
    pub const NORMAL: Self = Self { priority: 1_000 };
    pub const LOW: Self = Self { priority: 10_000 };
}

#[derive(Debug, Clone)]
pub struct EquivalentExpression {
    pub target_group: GroupId,
    pub key: LogicalExprKey,
    pub payload: LogicalPayloadId,
    /// Exact operator-shell equality witness for collision-safe Memo
    /// interning. Core-only rules may leave this absent when their synthetic
    /// fingerprint is itself the complete test-domain identity.
    pub operator_encoding: Option<Box<[u8]>>,
    pub logical_properties: super::memo::LogicalProperties,
    pub cardinality: super::memo::GroupCardinality,
    pub proof: EquivalenceProof,
}

impl EquivalentExpression {
    /// Consume the complete planner publication value at the Memo boundary.
    /// Structural identity and the fact snapshot travel together; callers do
    /// not reconstruct one from the other after the transformation commits.
    pub(crate) fn into_memo_insertion(self) -> super::memo::LogicalInsertionContract {
        super::memo::LogicalInsertionContract {
            target: self.target_group,
            key: self.key,
            payload: self.payload,
            operator_encoding: self.operator_encoding,
            proof: self.proof,
            logical_properties: self.logical_properties,
            cardinality: self.cardinality,
        }
    }
}

pub struct RuleContext<'a> {
    pub memo: &'a Memo,
    pub group: GroupId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternEnumerationCompletion {
    Complete,
    BudgetLimited {
        enumerated_bindings: usize,
        omitted_at_least: usize,
    },
}

/// One explicitly bound Memo operand. Expression nodes name the exact logical
/// alternative consumed by a matcher; group nodes are preserved holes which
/// a transformation deliberately does not inspect.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PatternOperand {
    Expression {
        group: GroupId,
        expression: LogicalExprId,
        children: Box<[PatternOperand]>,
    },
    Group(GroupId),
}

impl PatternOperand {
    pub fn expression(&self) -> Option<LogicalExprId> {
        match self {
            Self::Expression { expression, .. } => Some(*expression),
            Self::Group(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PatternBinding {
    pub root: PatternOperand,
    pub fingerprint: Fingerprint,
}

/// A resumable, exact continuation for a necessary-domain request which
/// reached a Memo group hole.  The binding is the work that may be resumed;
/// the remaining fields are the proof context that makes the resume safe.
/// In particular, a continuation is not a selected-winner certificate: its
/// reads are rechecked before it is scheduled and the normal transformation
/// verifier still owns publication.
#[derive(Debug, Clone)]
pub(crate) struct DomainContinuation {
    pub(crate) binding: PatternBinding,
    pub(crate) hole: GroupId,
    pub(crate) predicates: Box<[Expression]>,
    pub(crate) reads: Box<[PatternRead]>,
    pub(crate) occurrence: LogicalExprId,
    pub(crate) context: super::ids::OptimizationContextId,
}

impl PatternBinding {
    pub fn root_group(&self) -> GroupId {
        match &self.root {
            PatternOperand::Expression { group, .. } | PatternOperand::Group(group) => *group,
        }
    }

    pub fn root_expression(&self) -> LogicalExprId {
        self.root
            .expression()
            .expect("a transformation binding root must be an expression")
    }

    pub fn root_only(group: GroupId, expression: LogicalExprId, logical: &LogicalExpr) -> Self {
        Self {
            root: PatternOperand::Expression {
                group,
                expression,
                children: logical
                    .key
                    .children
                    .iter()
                    .copied()
                    .map(PatternOperand::Group)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            },
            fingerprint: logical.key.stable_fingerprint(),
        }
    }
}

/// Categories of Memo state observed by one task.
///
/// The old read cursor encoded the categories indirectly through two optional
/// frontier revisions while always taking both fact fingerprints.  That made
/// a structural reader subscribe to statistics even when it never consumed
/// them.  Keep the categories in the cursor itself so invalidation and task
/// identity describe the evidence actually used by the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadScope(u8);

impl ReadScope {
    pub const LOGICAL_FRONTIER: Self = Self(1 << 0);
    pub const PHYSICAL_FRONTIER: Self = Self(1 << 1);
    pub const LOGICAL_FACTS: Self = Self(1 << 2);
    pub const STATISTICS: Self = Self(1 << 3);
    pub const FRONTIERS: Self = Self(Self::LOGICAL_FRONTIER.0 | Self::PHYSICAL_FRONTIER.0);
    pub const FACTS: Self = Self(Self::LOGICAL_FACTS.0 | Self::STATISTICS.0);
    pub const ALL: Self = Self(Self::FRONTIERS.0 | Self::FACTS.0);

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PatternRead {
    pub group: GroupId,
    /// The exact Memo categories consumed by this read.
    pub scope: ReadScope,
    /// `Some` when the matcher enumerated this group's alternatives. A fixed
    /// root expression may consume only group facts/statistics; in that case
    /// peer root insertions must not invalidate and recursively wake the same
    /// binding task.
    pub logical_frontier_revision: Option<u64>,
    /// The complete goal whose physical frontier is consumed. The goal
    /// carries required properties, row goal, objective, grant, and context;
    /// it must never be replaced with a group-wide winner identity.
    pub physical_goal: Option<OptimizationGoal>,
    /// `Some` for physical tasks which consume the group's implementation
    /// domain. Adding a physical implementation can affect every goal, while
    /// publishing a winner affects only `physical_goal` below.
    pub physical_implementation_revision: Option<u64>,
    /// `Some` for physical tasks which consume the exact goal frontier. The
    /// revision is owned by that goal, so an unrelated child publication does
    /// not invalidate this task. It is deliberately absent from
    /// transformation reads.
    pub physical_frontier_revision: Option<u64>,
    pub logical_fact_fingerprint: Fingerprint,
    pub statistics_snapshot_fingerprint: Fingerprint,
}

impl PatternRead {
    pub fn from_group(memo: &Memo, group: GroupId) -> Result<Self> {
        Self::read(
            memo,
            group,
            ReadScope::LOGICAL_FRONTIER.union(ReadScope::FACTS),
            None,
        )
    }

    /// Read a group for a physical subproblem. In addition to the logical
    /// frontier and facts, the task observes physical publications so a
    /// parent cannot reuse a child frontier that changed during a nested
    /// optimization request.
    pub fn physical_from_group(
        memo: &Memo,
        group: GroupId,
        goal: OptimizationGoal,
    ) -> Result<Self> {
        Self::read(memo, group, ReadScope::ALL, Some(goal))
    }

    pub fn facts_from_group(memo: &Memo, group: GroupId) -> Result<Self> {
        Self::read(memo, group, ReadScope::FACTS, None)
    }

    /// Read only the logical structure of a group.  This is intended for
    /// matchers which do not inspect facts; a statistics update must not wake
    /// or invalidate such a task.
    pub fn structure_from_group(memo: &Memo, group: GroupId) -> Result<Self> {
        Self::read(memo, group, ReadScope::LOGICAL_FRONTIER, None)
    }

    fn read(
        memo: &Memo,
        group: GroupId,
        scope: ReadScope,
        requested_physical_goal: Option<OptimizationGoal>,
    ) -> Result<Self> {
        let group = memo.canonical_group(group);
        let group_ref = memo
            .group(group)
            .ok_or_else(|| paro_error::internal("rule binding read an unknown group"))?;
        let physical_goal = if scope.contains(ReadScope::PHYSICAL_FRONTIER) {
            Some(requested_physical_goal.ok_or_else(|| {
                paro_error::internal("physical read must identify the exact optimization goal")
            })?)
        } else {
            if requested_physical_goal.is_some() {
                return Err(paro_error::internal(
                    "non-physical read carried a physical goal",
                ));
            }
            None
        };
        // A cursor observes one recipe, not an unmetered recursive estimate.
        // Readers resolving inherited facts must subscribe to their inputs.
        Ok(Self {
            group,
            scope,
            logical_frontier_revision: scope.contains(ReadScope::LOGICAL_FRONTIER)
                .then(|| group_ref.logical_expression_version()),
            physical_goal,
            physical_implementation_revision: scope
                .contains(ReadScope::PHYSICAL_FRONTIER)
                .then(|| group_ref.physical_implementation_version()),
            physical_frontier_revision: physical_goal
                .map(|goal| group_ref.physical_frontier_version(goal)),
            logical_fact_fingerprint: if scope.contains(ReadScope::LOGICAL_FACTS) {
                group_ref.logical_fact_fingerprint()
            } else {
                Default::default()
            },
            statistics_snapshot_fingerprint: if scope.contains(ReadScope::STATISTICS) {
                memo.local_statistics_fingerprint(group)
            } else {
                Default::default()
            },
        })
    }

    pub fn is_current(self, memo: &Memo) -> Result<bool> {
        Ok(self.matches(Self::read(
            memo,
            self.group,
            self.scope,
            self.physical_goal,
        )?))
    }

    /// Publication uses the same exact read contract as task reuse. Physical
    /// children are immutable candidate frontiers, but a parent composed from
    /// an older child revision must not be recorded as a current completion;
    /// the engine retries that parent against a fresh targeted ReadSet.
    pub fn is_current_for_publication(self, memo: &Memo) -> Result<bool> {
        Ok(self.matches(Self::read(
            memo,
            self.group,
            self.scope,
            self.physical_goal,
        )?))
    }

    /// Combine observations of the same group without widening either one.
    /// The values for a category are taken from a cursor that actually
    /// observed that category; inactive fields remain zero and are ignored by
    /// [`Self::matches`].
    pub fn union(self, other: Self) -> Self {
        debug_assert_eq!(self.group, other.group);
        let scope = self.scope.union(other.scope);
        Self {
            group: self.group,
            scope,
            logical_frontier_revision: if other.scope.contains(ReadScope::LOGICAL_FRONTIER) {
                other.logical_frontier_revision
            } else {
                self.logical_frontier_revision
            },
            physical_goal: if other.scope.contains(ReadScope::PHYSICAL_FRONTIER) {
                other.physical_goal
            } else {
                self.physical_goal
            },
            physical_implementation_revision: if other
                .scope
                .contains(ReadScope::PHYSICAL_FRONTIER)
            {
                other.physical_implementation_revision
            } else {
                self.physical_implementation_revision
            },
            physical_frontier_revision: if other.scope.contains(ReadScope::PHYSICAL_FRONTIER) {
                other.physical_frontier_revision
            } else {
                self.physical_frontier_revision
            },
            logical_fact_fingerprint: if other.scope.contains(ReadScope::LOGICAL_FACTS) {
                other.logical_fact_fingerprint
            } else {
                self.logical_fact_fingerprint
            },
            statistics_snapshot_fingerprint: if other.scope.contains(ReadScope::STATISTICS) {
                other.statistics_snapshot_fingerprint
            } else {
                self.statistics_snapshot_fingerprint
            },
        }
    }

    /// Whether two observations can be represented by one cursor without
    /// losing a revision.  A read set may observe the same group more than
    /// once while a task is assembled; if the same category changed between
    /// those observations, both snapshots remain semantically significant.
    pub fn can_union(self, other: Self) -> bool {
        if self.group != other.group {
            return false;
        }
        let overlap = self.scope.0 & other.scope.0;
        (overlap & ReadScope::LOGICAL_FRONTIER.0 == 0
            || self.logical_frontier_revision == other.logical_frontier_revision)
            && (overlap & ReadScope::PHYSICAL_FRONTIER.0 == 0
                || (self.physical_goal == other.physical_goal
                    && self.physical_implementation_revision
                        == other.physical_implementation_revision
                    && self.physical_frontier_revision == other.physical_frontier_revision))
            && (overlap & ReadScope::LOGICAL_FACTS.0 == 0
                || self.logical_fact_fingerprint == other.logical_fact_fingerprint)
            && (overlap & ReadScope::STATISTICS.0 == 0
                || self.statistics_snapshot_fingerprint == other.statistics_snapshot_fingerprint)
    }

    fn matches(self, current: Self) -> bool {
        self.group == current.group
            && self.scope == current.scope
            && (!self.scope.contains(ReadScope::LOGICAL_FRONTIER)
                || self.logical_frontier_revision == current.logical_frontier_revision)
            && (!self.scope.contains(ReadScope::PHYSICAL_FRONTIER)
                || (self.physical_goal == current.physical_goal
                    && self.physical_implementation_revision
                        == current.physical_implementation_revision
                    && self.physical_frontier_revision == current.physical_frontier_revision))
            && (!self.scope.contains(ReadScope::LOGICAL_FACTS)
                || self.logical_fact_fingerprint == current.logical_fact_fingerprint)
            && (!self.scope.contains(ReadScope::STATISTICS)
                || self.statistics_snapshot_fingerprint == current.statistics_snapshot_fingerprint)
    }
}

#[derive(Debug, Clone)]
pub struct PatternBindingSet {
    pub bindings: Box<[PatternBinding]>,
    pub reads: Box<[PatternRead]>,
    /// Actual matcher work admitted before any operand is allocated. One unit
    /// represents either an observed Memo group or a constructed operand.
    /// This is deliberately distinct from `reads`: a shared DAG can have a
    /// small read set while requiring exponentially many tree operands.
    pub work_units: usize,
    /// Ledger dimension charged for `work_units`. Parent composition rules use
    /// an isolated pool so local expansion cannot prevent them from observing
    /// already-published child alternatives.
    pub work_dimension: BudgetDimension,
    pub completion: PatternEnumerationCompletion,
}

/// Allocation-free root dispatch plus the exact reads which justify a
/// negative result.  Most rules only need the immutable shell predicate and
/// therefore return an empty read set.  Planner rules with an existential
/// path witness may return a completed, dependency-aware negative lookup so
/// the engine can avoid re-running the full matcher while still waking the
/// task when a child frontier changes.
#[derive(Debug, Clone, Default)]
pub struct RootDispatch {
    pub matches: bool,
    pub reads: Box<[PatternRead]>,
}

/// A binding-local, fail-closed result that can be established from the
/// immutable binding and already-observed Memo facts before entering a rule's
/// construction transaction.  `NoOutput` is deliberately weaker than an
/// output identity: it may suppress only a binding which the rule can prove
/// cannot produce any legal alternative.  Rules which cannot make that proof
/// must return `Continue` and keep the authoritative apply path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformationPreflight {
    Continue,
    NoOutput,
}

type TransformationRollback = Box<dyn FnOnce() -> Result<()> + 'static>;

pub struct TransformContext<'a> {
    memo: &'a mut Memo,
    group: GroupId,
    memo_savepoint: Option<super::memo::TransformationSavepoint>,
    sidecar_rollbacks: Vec<TransformationRollback>,
    fact_reads: BTreeMap<GroupId, PatternRead>,
    fact_value_fingerprint: Option<Fingerprint>,
    domain_continuations: Vec<DomainContinuation>,
    domain_continuations_enabled: bool,
    pub(crate) rejection_reasons: Option<crate::transformation_rejection::RejectionReasons>,
}

impl<'a> TransformContext<'a> {
    pub(crate) fn new(memo: &'a mut Memo, group: GroupId) -> Self {
        Self {
            memo,
            group,
            memo_savepoint: None,
            sidecar_rollbacks: Vec::new(),
            fact_reads: BTreeMap::new(),
            fact_value_fingerprint: None,
            domain_continuations: Vec::new(),
            domain_continuations_enabled: false,
            rejection_reasons: None,
        }
    }

    pub fn memo(&self) -> &Memo {
        self.memo
    }

    pub fn group(&self) -> GroupId {
        self.group
    }

    /// Admit evidence work before traversal or allocation. Unlike a Memo
    /// mutation, this work survives an unsuccessful transformation attempt.
    pub fn admit_fact_work(&mut self, dimension: BudgetDimension, units: usize) -> Result<bool> {
        if !self.memo.control().checkpoint()? {
            return Ok(false);
        }
        if units == 0 {
            return Ok(true);
        }
        if self.memo_savepoint.is_some() {
            return Err(paro_error::internal(
                "facts must be read before transformation publication",
            ));
        }
        let ledger = self
            .memo
            .group_ledger_mut(self.group)
            .ok_or_else(|| paro_error::internal("fact reader lost its owner"))?;
        let decision = ledger.admit_executed_work(
            dimension,
            u32::try_from(units).unwrap_or(u32::MAX),
            |consumed| {
                let mut event = StableFingerprintBuilder::default();
                event.write_bytes(b"paro.memo.fact-read-work.v1");
                event.write_u64(self.group.0 as u64);
                event.write_u64(consumed as u64);
                event.finish()
            },
        );
        match decision {
            super::budget::BudgetDecision::Allowed => Ok(true),
            super::budget::BudgetDecision::Exhausted => Ok(false),
            _ => Err(paro_error::internal(
                "fact work requires a configured occurrence budget",
            )),
        }
    }

    pub fn record_fact_read(&mut self, read: PatternRead) {
        if let Some(previous) = self.fact_reads.get_mut(&read.group) {
            *previous = previous.union(read);
        } else {
            self.fact_reads.insert(read.group, read);
        }
    }

    pub(crate) fn take_fact_reads(&mut self) -> Vec<PatternRead> {
        std::mem::take(&mut self.fact_reads).into_values().collect()
    }

    /// Enable same-Memo demand continuations for an explicitly selected
    /// quality binding.  Ordinary transformation tasks keep this disabled so
    /// the accelerator cannot alter the complete search frontier or pay for
    /// group-shell inspection on the normal matcher lane.
    pub(crate) fn enable_domain_continuations(&mut self) {
        self.domain_continuations_enabled = true;
    }

    pub(crate) fn domain_continuations_enabled(&self) -> bool {
        self.domain_continuations_enabled
    }

    pub(crate) fn record_domain_continuations(
        &mut self,
        continuations: impl IntoIterator<Item = DomainContinuation>,
    ) {
        self.domain_continuations.extend(continuations);
    }

    pub(crate) fn take_domain_continuations(&mut self) -> Vec<DomainContinuation> {
        std::mem::take(&mut self.domain_continuations)
    }

    /// Record the canonical value of the facts consumed by this binding.
    /// Revalidation may then advance revision cursors without re-running the
    /// transformation when a different fact recipe resolves to the same value.
    pub fn record_fact_value(&mut self, fingerprint: Fingerprint) {
        self.fact_value_fingerprint = Some(fingerprint);
    }

    pub(crate) fn fact_value_fingerprint(&self) -> Option<Fingerprint> {
        self.fact_value_fingerprint
    }

    /// Obtain the bounded transformation writer. The Memo snapshot is created
    /// only at the first write, so failed shape checks do not clone the region
    /// forest. Transformations may append groups/expressions and update region
    /// facets, but must not mutate physical search state.
    pub fn memo_mut(&mut self) -> &mut Memo {
        if self.memo_savepoint.is_none() {
            self.memo_savepoint = Some(self.memo.transformation_savepoint());
        }
        self.memo
    }

    /// Enlist optimizer-owned side state in the same attempt as Memo writes.
    /// The action is registered immediately before the first side-state write
    /// and runs in reverse registration order on every rejected output path.
    pub fn enlist_rollback(&mut self, rollback: impl FnOnce() -> Result<()> + 'static) {
        self.sidecar_rollbacks.push(Box::new(rollback));
    }

    /// Mutate Memo and one optimizer-owned sidecar under a single attempt.
    /// The sidecar rollback is registered before either state can be written,
    /// and the mutation closure receives the write guard directly so it never
    /// needs to re-enter the same lock.
    pub fn with_sidecar_transaction<S, Savepoint, Output>(
        &mut self,
        sidecar: Arc<RwLock<S>>,
        savepoint: impl FnOnce(&S) -> Savepoint,
        rollback: impl FnOnce(&mut S, Savepoint) -> Result<()> + Send + 'static,
        mutate: impl FnOnce(&mut Memo, &mut S) -> Result<Output>,
    ) -> Result<Output>
    where
        S: Send + Sync + 'static,
        Savepoint: Send + 'static,
    {
        let mut state = sidecar
            .write()
            .map_err(|_| paro_error::internal("transformation sidecar state poisoned"))?;
        let checkpoint = savepoint(&state);
        let rollback_state = sidecar.clone();
        self.enlist_rollback(move || {
            let mut state = rollback_state.write().map_err(|_| {
                paro_error::internal("transformation sidecar state poisoned during rollback")
            })?;
            rollback(&mut state, checkpoint)
        });
        if self.memo_savepoint.is_none() {
            self.memo_savepoint = Some(self.memo.transformation_savepoint());
        }
        mutate(self.memo, &mut state)
    }

    pub(crate) fn rollback(mut self) -> Result<()> {
        let _partition = crate::work_partition::enter(crate::work_partition::Bucket::Apply);
        let mut failures = Vec::new();
        while let Some(rollback) = self.sidecar_rollbacks.pop() {
            if let Err(error) = rollback() {
                failures.push(format!("sidecar rollback failed: {error}"));
            }
        }
        if let Some(savepoint) = self.memo_savepoint.take() {
            if let Err(error) = self.memo.rollback_transformation(savepoint) {
                failures.push(format!("Memo rollback failed: {error}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(paro_error::internal(format!(
                "transformation transaction rollback left inconsistent state: {}",
                failures.join("; ")
            )))
        }
    }

    pub(crate) fn commit(mut self) -> Result<(Box<[GroupId]>, BTreeSet<GroupId>)> {
        let _partition = crate::work_partition::enter(crate::work_partition::Bucket::Insert);
        let Some(savepoint) = self.memo_savepoint.take() else {
            return Ok((Box::new([]), BTreeSet::new()));
        };
        let appended_groups = self.memo.appended_groups_since(&savepoint)?;
        let written_groups = self.memo.take_transformation_written_groups();
        Ok((appended_groups, written_groups))
    }
}

pub trait TransformationRule: Send + Sync {
    fn id(&self) -> RuleId;

    /// Declare a semantic producer dependency for the optional quality-first
    /// lane.  Rules without a declaration remain on the ordinary lane and are
    /// never treated as evidence that a quality chain is complete.
    fn quality_dependency(&self) -> Option<QualityDependency> {
        None
    }

    /// Evidence consumed when applying one exact binding. Discovery reads may
    /// include unrelated alternatives; rules that consume only their bound
    /// operands can narrow this to the operands' facts. The default retains
    /// every discovery dependency for rules with additional Memo reads.
    fn binding_reads(
        &self,
        _binding: &PatternBinding,
        discovery_reads: &[PatternRead],
        _ctx: &RuleContext<'_>,
    ) -> Result<Box<[PatternRead]>> {
        Ok(discovery_reads.into())
    }

    /// Resolve the canonical value of facts consumed by one binding. This is
    /// called only after an earlier application's revision cursor became
    /// stale. Returning the same value proves the prior result (including a
    /// no-match) remains valid and avoids repeating the rewrite. The default
    /// retains revision-based invalidation for rules without value facts.
    fn binding_fact_value(
        &self,
        _binding: &PatternBinding,
        _ctx: &mut TransformContext<'_>,
    ) -> Result<Option<Fingerprint>> {
        Ok(None)
    }

    /// Return exact bindings for a quality obligation already observed in one
    /// frozen candidate.  This is an ordering hint only: the ordinary rule
    /// matcher still owns the complete legal search space.  The default keeps
    /// rules which have no native selected-path contract off this lane.
    fn selected_quality_bindings(
        &self,
        _memo: &Memo,
        _candidate: &FrozenCandidate,
    ) -> Result<Box<[PatternBinding]>> {
        Ok(Box::new([]))
    }

    /// Allocation-free dispatch predicate over the immutable expression
    /// shell. Implementations must not inspect child groups here: a false
    /// result means descendant changes can never make this rule applicable,
    /// so the scheduler deliberately records no dependency subscriptions.
    fn matches_root(&self, expr: &LogicalExpr) -> bool;

    /// Allocation-free root capability filter used by the scheduler before
    /// it constructs a full dispatch/read witness. `true` is the conservative
    /// default: custom rules which cannot describe their root domain remain
    /// on the complete path. A planner rule may return `false` only for an
    /// immutable operator tag that can never satisfy the rule; descendant
    /// frontiers are not inspected by this accelerator.
    fn root_operator_tag_may_match(&self, _operator_tag: Option<u64>) -> bool {
        true
    }

    /// Dispatch one task using the immutable root shell and, when available,
    /// a completed dependency-aware negative witness lookup.  The default is
    /// deliberately equivalent to `matches_root`; custom rules do not need
    /// to adopt the planner's path-index contract just to participate in the
    /// engine.
    fn root_dispatch(&self, expr: &LogicalExpr, _ctx: &RuleContext<'_>) -> Result<RootDispatch> {
        Ok(RootDispatch {
            matches: self.matches_root(expr),
            reads: Box::new([]),
        })
    }

    /// Perform a conservative binding-local check before allocating the
    /// rule's construction transaction.  This is not a semantic recognizer or
    /// an output identity: `NoOutput` is valid only when the exact binding
    /// cannot produce a legal result without inspecting any additional Memo
    /// alternative.  The default preserves the complete rule path.
    fn preflight_binding(
        &self,
        _binding: &PatternBinding,
        _ctx: &RuleContext<'_>,
    ) -> Result<TransformationPreflight> {
        Ok(TransformationPreflight::Continue)
    }

    /// Maximum number of alternatives one firing may publish. Local rewrite
    /// rules keep the default of one. A bounded whole-region owner may expose
    /// a deterministic frontier, but the engine reserves every possible root
    /// expression before the rule mutates Memo or sidecar state.
    /// Only the admitted binding's shells may be inspected here; discovering
    /// new child alternatives belongs to pattern enumeration and its budget.
    fn output_bound(&self, _binding: &PatternBinding, _ctx: &RuleContext<'_>) -> usize {
        1
    }

    /// Whether one successful binding publishes that rule's complete output
    /// frontier for the observed read set. The engine may seed the produced
    /// expression with the same read cursor, but must invalidate it whenever
    /// any observed frontier or fact advances. This is an incremental-work
    /// contract, not an equivalence/provenance predicate.
    fn output_saturates_observed_binding(&self) -> bool {
        false
    }

    fn promise(&self, _expr: &LogicalExpr, _ctx: &RuleContext<'_>) -> RulePromise {
        RulePromise::NORMAL
    }

    /// Admission class used by binding enumeration, rule firing, output
    /// reservation, and newly staged groups. The engine applies this single
    /// declaration to the complete optional transaction.
    fn budget_class(&self) -> TransformationBudgetClass {
        TransformationBudgetClass::Local
    }

    fn matches(&self, expr: &LogicalExpr, ctx: &RuleContext<'_>) -> bool;

    /// Bind the exact alternatives consumed by this firing and return every
    /// frontier revision read while doing so, including a completed no-match.
    /// Rules which only inspect their root inherit the root binding and direct
    /// child read set; structural planner rules override this with native
    /// pattern enumeration.
    fn bindings(&self, expr: LogicalExprId, ctx: &RuleContext<'_>) -> Result<PatternBindingSet> {
        let logical = ctx
            .memo
            .logical_expr(expr)
            .ok_or_else(|| paro_error::internal("rule binding lost its root expression"))?;
        let reads = std::iter::once(PatternRead::facts_from_group(ctx.memo, ctx.group))
            .chain(
                logical
                    .key
                    .children
                    .iter()
                    .copied()
                    .map(|group| PatternRead::from_group(ctx.memo, group)),
            )
            .collect::<Result<Vec<_>>>()?;
        let bindings = self
            .matches(logical, ctx)
            .then(|| PatternBinding::root_only(ctx.group, expr, logical))
            .into_iter()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(PatternBindingSet {
            bindings,
            reads: reads.into_boxed_slice(),
            work_units: 1 + logical.key.children.len(),
            work_dimension: self.budget_class().work_dimension(),
            completion: PatternEnumerationCompletion::Complete,
        })
    }

    fn apply(
        &self,
        expr: LogicalExprId,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>>;

    fn apply_binding(
        &self,
        binding: &PatternBinding,
        ctx: &mut TransformContext<'_>,
    ) -> Result<Box<[EquivalentExpression]>> {
        self.apply(binding.root_expression(), ctx)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GrantDependencyDescriptor {
    /// Neither memory-class decisions nor worker capacity affect this local
    /// implementation. Children can still make the enclosing group sensitive.
    Invariant,
    /// Memory independent, but source supply/latency depends on the worker
    /// capacity. Equal capacities may share a winner across memory classes.
    Parallelism,
    /// The complete operating point is required (including memory/spill).
    Sensitive,
}

/// Query-local identity of one base row source. Binder table indexes are
/// unique across aliases, so self joins remain distinct while equivalent Memo
/// expressions retain the same source identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WorkSourceId(pub usize);

/// Stable identity of one survivor-domain proof. It names the build domain,
/// probe-key mapping, equality/NULL semantics, and statistics snapshot used
/// to derive a source-local retention bound. Reusing the same proof cannot
/// shrink the source domain twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DomainProofId(pub Fingerprint);

/// Stable identity of one physical predicate evaluation. This is deliberately
/// separate from [`DomainProofId`]: two operators may evaluate the same domain
/// proof, while replaying one operator during search must not charge it twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EvaluationOccurrenceId(pub Fingerprint);

/// Source-local work retention derived for one runtime-filter installation.
/// Keeping the ratio beside its source preserves a unique-key proof on one
/// lineage even when another lineage of the same join key is non-unique.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SidewaysFilterSource {
    pub source: WorkSourceId,
    /// Identity of the exact domain proof. Replaying the same proof is
    /// idempotent; different identities are conservatively correlated unless
    /// a future joint-domain proof explicitly relates them.
    pub domain: DomainProofId,
    /// Physical evaluation which publishes `domain` to this source.
    pub evaluation: EvaluationOccurrenceId,
    pub expected_retained_ppm: u32,
    pub upper_retained_ppm: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRetentionProof {
    pub domain: DomainProofId,
    pub expected_retained_ppm: u32,
    /// Absolute survivor bound relative to the immutable base source, never a
    /// conditional selectivity relative to the preceding filter.
    pub upper_retained_ppm: u32,
}

/// A disjoint portion of a winner's work proven to belong to one base source.
/// The contained cost is work-only: memory and external-resource contracts
/// remain on the complete winner and are never weakened by selectivity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SourceFilterWork {
    pub domain: DomainProofId,
    pub evaluation: EvaluationOccurrenceId,
    /// Immutable input-row domain on which this evaluation is charged.  This
    /// is intentionally carried next to the occurrence rather than inferred
    /// from `SourceWork::cost`: the latter already contains survivor
    /// reductions and therefore changes with join composition order.
    pub evaluation_rows: u64,
    pub expected_retained_ppm: u32,
    pub upper_retained_ppm: u32,
    /// Cost of evaluating this predicate against the unfiltered source.  It is
    /// allocated once from the operator-local full-source term and then only
    /// scaled by preceding *distinct* domain proofs.  It must never be derived
    /// from a lane's already-retained `cost`.
    pub full_apply_cost: SearchCost,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceWork(Arc<SourceWorkData>);

impl SourceWork {
    pub(crate) fn snapshot(&self) -> &SourceWorkData {
        &self.0
    }

    /// Diagnostic-only allocation identity, valid while the snapshot is
    /// borrowed. This is never a semantic identity or retained cache key.
    pub(crate) fn payload_identity(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    pub(crate) fn retained_payload_bytes(&self) -> usize {
        std::mem::size_of::<SourceWorkData>()
            + std::mem::size_of_val(self.retentions.as_ref())
            + std::mem::size_of_val(self.filters.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn shares_payload(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl From<SourceWorkData> for SourceWork {
    fn from(data: SourceWorkData) -> Self {
        Self(Arc::new(data))
    }
}

impl std::ops::Deref for SourceWork {
    type Target = SourceWorkData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An immutable source response once published. Streaming/branch composition
/// shares the complete snapshot; only a filter which changes this source
/// constructs a new one. No mutable access to a published snapshot is exposed.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceWorkData {
    pub source: WorkSourceId,
    /// Immutable number of rows in this physical source lane before runtime
    /// predicates. Predicate work is attributed by this row domain, never by
    /// byte cost or by a cost already reduced by an earlier predicate.
    pub source_rows: u64,
    /// Work of the unfiltered source. Every survivor proof is interpreted in
    /// this immutable domain so correlated filters cannot multiply hard bounds.
    pub base_cost: SearchCost,
    /// Base-source access work after every selected runtime filter.
    pub cost: SearchCost,
    /// Unique survivor proofs already applied to the base domain.
    pub retentions: Box<[SourceRetentionProof]>,
    /// Runtime predicates already attached to this source.
    pub filters: Box<[SourceFilterWork]>,
    /// Jointly ordered evaluation work currently present in the winner cost.
    pub filter_apply_cost: SearchCost,
    /// Complete source-pipeline work after retention and predicate ordering,
    /// folded once at the pipeline operating point.
    pub phased_cost: SearchCost,
    pub phase_tasks: u16,
}

#[derive(Debug, Clone)]
pub struct ChildGoalAlternative {
    pub children: Box<[(GroupId, OptimizationGoal)]>,
}

/// Physical lifecycle used when composing a candidate with its children.
/// The mask names child pipelines whose peak can overlap operator-owned state;
/// total work and critical path still follow the dependency order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CostComposition {
    /// Children exist only to carry a schema and are never scheduled.
    LocalOnly,
    /// A base scan establishes one source-work lane. Ancestors propagate this
    /// lane without guessing which of their own local work is source-driven.
    Source {
        source: WorkSourceId,
        source_rows: u64,
    },
    Sequential,
    RetainedState {
        overlapping_children: u64,
    },
    /// A region-owned side input reduces work inside one child boundary.
    /// Resource proofs remain unscaled; the integer ratios affect only
    /// estimated/risk work and are recorded in the JointCostProof.
    SidewaysFilter {
        overlapping_children: u64,
        filtered_child: u8,
        sources: Box<[SidewaysFilterSource]>,
    },
}

/// Physical pipeline/phase contract used to price span after child winners
/// are known. Total work remains in `SearchCost`; this contract only assigns
/// that work to real scheduler phases and carries the resulting output supply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSupplyContract {
    /// Utility, finish, and other intrinsically single-task work.
    Serial,
    /// A physical source creates a new pipeline task domain.
    Source { tasks: u16 },
    /// A streaming transform executes in, and preserves, one child pipeline.
    Streaming { input: u8 },
    /// A breaker consumes an input pipeline and creates a distinct emit
    /// source. `output_tasks` comes from that source representation.
    Breaker {
        input: u8,
        output_tasks: u16,
        profile: ParallelWorkProfile,
    },
    /// Hash/cross-product execution has a build phase and a probe phase. The
    /// split is expressed in calibrated work ppm, not row or byte cardinality,
    /// and therefore conserves the immutable work vector exactly.
    BuildProbe {
        build: u8,
        probe: u8,
        build_work_ppm: u32,
    },
}

impl TaskSupplyContract {
    pub const fn serial() -> Self {
        Self::Serial
    }
}

impl CostComposition {
    pub(crate) fn overlapping_children(&self) -> u64 {
        match self {
            Self::LocalOnly | Self::Source { .. } | Self::Sequential => 0,
            Self::RetainedState {
                overlapping_children,
            }
            | Self::SidewaysFilter {
                overlapping_children,
                ..
            } => *overlapping_children,
        }
    }

    pub(crate) fn sideways_filter(&self) -> Option<(usize, &[SidewaysFilterSource])> {
        match self {
            Self::SidewaysFilter {
                filtered_child,
                sources,
                ..
            } => Some((usize::from(*filtered_child), sources.as_ref())),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCandidate {
    pub key: PhysicalExprKey,
    pub payload: PhysicalPayloadId,
    pub provided: ProvidedProperties,
    pub child_goals: Box<[(GroupId, OptimizationGoal)]>,
    pub local_cost: SearchCost,
    /// Work already included in `local_cost` for evaluating this candidate's
    /// runtime predicate against an otherwise unfiltered source.
    pub source_filter_apply_cost: Option<SearchCost>,
    pub task_supply: TaskSupplyContract,
    pub cost_composition: CostComposition,
    /// Whether operator-owned retained state can yield memory to spill after
    /// child winner peaks are known.
    pub spillable: bool,
    pub enforcer_cost_input: super::engine::EnforcerCostInput,
    pub physical_fingerprint: Fingerprint,
    pub region: Option<RegionCandidateContract>,
    /// Every logical expression must have at least one mandatory implementation
    /// path. Optional candidates may be cut by the search ledger.
    pub mandatory: bool,
}

impl PhysicalCandidate {
    pub fn stable_event(&self, goal: OptimizationGoal) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_fingerprint(self.key.stable_fingerprint());
        builder.write_fingerprint(self.physical_fingerprint);
        builder.write_u64(goal.required.0 as u64);
        builder.write_u64(goal.row_goal.stable_tag());
        builder.write_u64(goal.objective.stable_tag());
        builder.write_u64(goal.grant.stable_tag());
        builder.write_u64(goal.context.0 as u64);
        if let Some(region) = &self.region {
            // RegionId is a normalized-forest position, not a stable
            // candidate identity. Keep budget events invariant under region
            // reindexing and use the stable facet declaration instead.
            let mut facets = region.facets.to_vec();
            facets.sort_unstable();
            facets.dedup();
            for facet in facets {
                builder.write_fingerprint(facet);
            }
            for artifact in &region.artifacts {
                builder.write_fingerprint(artifact.fingerprint);
                builder.write_u64(artifact.kind as u64);
            }
            for dependency in &region.artifact_dependencies {
                builder.write_fingerprint(dependency.artifact);
                let (producer_kind, producer_value) = dependency.producer.stable_tag();
                builder.write_u64(producer_kind);
                builder.write_u64(producer_value);
                let (consumer_kind, consumer_value) = dependency.consumer.stable_tag();
                builder.write_u64(consumer_kind);
                builder.write_u64(consumer_value);
                builder.write_u64(dependency.kind as u64);
            }
        }
        builder.finish()
    }
}

pub struct ImplementationContext<'a> {
    pub memo: &'a Memo,
    pub group: GroupId,
}

pub trait PhysicalImplementation: Send + Sync {
    fn id(&self) -> ImplementationId;

    fn grant_dependency(&self) -> GrantDependencyDescriptor {
        GrantDependencyDescriptor::Invariant
    }

    /// Conservative declaration used before per-class goal creation. An
    /// implementation may specialize this by logical operator, but returning
    /// `Invariant` is a correctness promise for every expression it can
    /// implement, not merely a costing hint.
    fn grant_dependency_for(
        &self,
        _expr: &LogicalExpr,
        _ctx: &ImplementationContext<'_>,
    ) -> GrantDependencyDescriptor {
        self.grant_dependency()
    }

    fn promise(&self, _expr: &LogicalExpr, _goal: OptimizationGoal) -> RulePromise {
        RulePromise::NORMAL
    }

    fn matches(
        &self,
        expr: &LogicalExpr,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> bool;

    fn candidates(
        &self,
        expr: LogicalExprId,
        goal: OptimizationGoal,
        ctx: &ImplementationContext<'_>,
    ) -> Result<Box<[PhysicalCandidate]>>;
}

#[derive(Default)]
pub struct ImplementationRegistry {
    transformations: BTreeMap<RuleId, Box<dyn TransformationRule>>,
    implementations: BTreeMap<ImplementationId, Box<dyn PhysicalImplementation>>,
}

impl std::fmt::Debug for ImplementationRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImplementationRegistry")
            .field("transformation_ids", &self.transformations.keys())
            .field("implementation_ids", &self.implementations.keys())
            .finish()
    }
}

impl ImplementationRegistry {
    pub fn register_transformation(
        &mut self,
        rule: impl TransformationRule + 'static,
    ) -> Result<()> {
        let id = rule.id();
        if self.transformations.insert(id, Box::new(rule)).is_some() {
            return Err(paro_error::internal(
                "duplicate stable transformation RuleId",
            ));
        }
        Ok(())
    }

    pub fn register_implementation(
        &mut self,
        implementation: impl PhysicalImplementation + 'static,
    ) -> Result<()> {
        let id = implementation.id();
        if self
            .implementations
            .insert(id, Box::new(implementation))
            .is_some()
        {
            return Err(paro_error::internal(
                "duplicate stable physical ImplementationId",
            ));
        }
        Ok(())
    }

    pub fn transformations(&self) -> impl ExactSizeIterator<Item = &dyn TransformationRule> {
        self.transformations.values().map(Box::as_ref)
    }

    pub fn implementations(&self) -> impl ExactSizeIterator<Item = &dyn PhysicalImplementation> {
        self.implementations.values().map(Box::as_ref)
    }

    pub fn transformation(&self, id: RuleId) -> Option<&dyn TransformationRule> {
        self.transformations.get(&id).map(Box::as_ref)
    }

    pub fn implementation(&self, id: ImplementationId) -> Option<&dyn PhysicalImplementation> {
        self.implementations.get(&id).map(Box::as_ref)
    }

    pub(crate) fn implementation_entries(
        &self,
    ) -> impl ExactSizeIterator<Item = (ImplementationId, &dyn PhysicalImplementation)> {
        self.implementations
            .iter()
            .map(|(id, implementation)| (*id, implementation.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transformation_rule_names_are_stable_and_bijective() {
        for &(id, name) in TRANSFORMATION_RULE_NAMES {
            assert_eq!(transformation_rule_name(id), Some(name));
            assert_eq!(transformation_rule_id(name), Some(id));
        }
        assert_eq!(
            transformation_rule_id("JOIN_ELIMINATION"),
            Some(JOIN_ELIMINATION_RULE)
        );
        assert!(transformation_rule_id("unknown").is_none());
    }
}
