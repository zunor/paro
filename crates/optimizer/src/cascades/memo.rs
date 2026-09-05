// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Contextual Memo with expression-local rule history and goal-keyed winners.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::budget::{BudgetDimension, SearchBudget, SearchLedger};
use super::calibration::MachineCalibrationBundle;
use super::column::GroupSchema;
use super::cost::SearchCost;
use super::enforcer::{replay_enforcer_chain, EnforcerStep};
use super::ids::{
    AdmissibleGrantSetId, Fingerprint, GroupId, ImplementationId, LogicalExprId, LogicalPayloadId,
    ObjectiveProfileId, OptimizationContextId, PhysicalExprId, PhysicalPayloadId, PropertySetId,
    ResourceGrantClassId, RuleId, StableFingerprintBuilder,
};
use super::properties::{PropertyInterner, ProvidedProperties, RequiredProperties};
use super::region::{JointCostProof, RegionFacet, RegionForest};
use super::rules::CostComposition;
use std::sync::Arc;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalProperties {
    pub unique_keys: BTreeSet<Box<[super::ids::ColumnId]>>,
    pub outer_references: BTreeSet<super::ids::ColumnId>,
    pub maximum_cardinality: Option<u64>,
}

impl LogicalProperties {
    pub fn same_contract(&self, other: &Self) -> bool {
        self.unique_keys == other.unique_keys && self.outer_references == other.outer_references
    }

    pub fn merge_equivalent_facts(&mut self, other: &Self) {
        self.maximum_cardinality = match (self.maximum_cardinality, other.maximum_cardinality) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound),
            (None, None) => None,
        };
    }
}

/// Declarative provenance and precedence of a group-level estimation recipe.
///
/// The order is semantic: later variants may replace earlier ones during a
/// true group merge. A recipe fingerprint identifies evidence but never ranks
/// its quality.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum CardinalityRecipeKind {
    #[default]
    Statistics,
    /// An exact row-count dependency on a row-preserving input group.
    RowPreservingInput,
    /// A rewrite whose equivalence proof exposes stronger relational-domain
    /// information to the estimator (for example aggregate subsumption).
    ConstraintRefined,
    /// A joint estimator over an associative region.
    JoinRegion,
}

/// Canonical, expression-independent cardinality estimate for one Memo group.
///
/// `recipe` identifies relational estimation evidence, not a physical winner.
/// Shape-only alternatives inherit the current group recipe; a transformation
/// may refine it only through an explicitly declared [`CardinalityRecipeKind`].
/// A true group merge combines peer uncertainty deterministically, so estimates
/// cannot depend on rule scheduling or the eventual physical winner.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupCardinality {
    /// Stable witness for diagnostics only. It never participates in estimate
    /// selection; peer merges retain the minimum solely as an associative,
    /// commutative and idempotent summary.
    recipe: Fingerprint,
    pub kind: CardinalityRecipeKind,
    /// The uncertainty hull of direct estimators at the selected recipe kind.
    range: Option<CardinalityEnvelope>,
    /// Semantic row-preserving dependencies. Multiple equivalent expressions
    /// may expose different input groups; retaining the complete bounded set
    /// makes group merging associative and lets their current estimates form
    /// an uncertainty hull during costing.
    inputs: BTreeSet<GroupId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CardinalityEnvelope {
    pub lower: u64,
    pub expected_lower: u64,
    pub expected_upper: u64,
    pub upper: u64,
}

impl GroupCardinality {
    pub fn new(
        recipe: Fingerprint,
        kind: CardinalityRecipeKind,
        lower: u64,
        expected: u64,
        upper: u64,
    ) -> Self {
        Self {
            recipe,
            kind,
            range: Some(CardinalityEnvelope {
                lower,
                expected_lower: expected,
                expected_upper: expected,
                upper,
            }),
            inputs: BTreeSet::new(),
        }
    }

    pub fn unknown(recipe: Fingerprint, kind: CardinalityRecipeKind) -> Self {
        Self {
            recipe,
            kind,
            range: None,
            inputs: BTreeSet::new(),
        }
    }

    pub fn inherit(recipe: Fingerprint, input: GroupId) -> Self {
        Self {
            recipe,
            kind: CardinalityRecipeKind::RowPreservingInput,
            range: None,
            inputs: BTreeSet::from([input]),
        }
    }

    pub fn with_kind(mut self, kind: CardinalityRecipeKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn canonical_with(self, other: Self) -> Self {
        let self_available = self.range.is_some() || !self.inputs.is_empty();
        let other_available = other.range.is_some() || !other.inputs.is_empty();
        match (self_available, other_available) {
            (false, true) => return other,
            (true, false) => return self,
            (false, false) => {
                return match self.kind.cmp(&other.kind) {
                    std::cmp::Ordering::Less => other,
                    std::cmp::Ordering::Greater => self,
                    std::cmp::Ordering::Equal => {
                        Self::unknown(self.recipe.min(other.recipe), self.kind)
                    }
                };
            }
            (true, true) => {}
        }
        match self.kind.cmp(&other.kind) {
            std::cmp::Ordering::Less => other,
            std::cmp::Ordering::Greater => self,
            std::cmp::Ordering::Equal => {
                let range = match (self.range, other.range) {
                    (Some(left), Some(right)) => Some(left.hull(right)),
                    (Some(range), None) | (None, Some(range)) => Some(range),
                    (None, None) => None,
                };
                let mut inputs = self.inputs;
                inputs.extend(other.inputs);
                Self {
                    // This value summarizes provenance; it does not elect the
                    // estimate associated with either peer recipe.
                    recipe: self.recipe.min(other.recipe),
                    kind: self.kind,
                    range,
                    inputs,
                }
            }
        }
    }

    pub fn representative(&self) -> Option<(u64, u64, u64)> {
        let range = self.range?;
        let expected = range
            .expected_lower
            .saturating_add(range.expected_upper.saturating_sub(range.expected_lower) / 2);
        Some((range.lower, expected, range.upper))
    }
}

impl CardinalityEnvelope {
    fn hull(self, other: Self) -> Self {
        Self {
            lower: self.lower.min(other.lower),
            expected_lower: self.expected_lower.min(other.expected_lower),
            expected_upper: self.expected_upper.max(other.expected_upper),
            upper: self.upper.max(other.upper),
        }
    }

    fn clamp(mut self, maximum: Option<u64>) -> Self {
        if let Some(maximum) = maximum {
            self.lower = self.lower.min(maximum);
            self.expected_lower = self.expected_lower.min(maximum).max(self.lower);
            self.expected_upper = self.expected_upper.min(maximum).max(self.expected_lower);
            self.upper = self.upper.min(maximum).max(self.expected_upper);
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum EquivalenceProof {
    Initial,
    Normalization {
        rule: RuleId,
    },
    Transformation {
        rule: RuleId,
        source: LogicalExprId,
        premise: Fingerprint,
    },
    SpecializedEnumerator {
        rule: RuleId,
        region: Fingerprint,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogicalExprKey {
    pub operator: Fingerprint,
    pub scalars: Box<[super::ids::ScalarExprId]>,
    pub children: Box<[GroupId]>,
}

impl LogicalExprKey {
    pub fn stable_fingerprint(&self) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_fingerprint(self.operator);
        builder.write_u64(self.scalars.len() as u64);
        for scalar in &self.scalars {
            builder.write_u64(scalar.0 as u64);
        }
        builder.write_u64(self.children.len() as u64);
        for child in &self.children {
            builder.write_u64(child.0 as u64);
        }
        builder.finish()
    }
}

#[derive(Debug, Clone)]
pub struct LogicalExpr {
    pub id: LogicalExprId,
    pub key: LogicalExprKey,
    pub payload: LogicalPayloadId,
    pub proofs: BTreeSet<EquivalenceProof>,
    pub applied_rules: BTreeSet<RuleId>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PhysicalExprKey {
    pub implementation: ImplementationId,
    pub logical: LogicalExprId,
    pub children: Box<[GroupId]>,
    pub payload_fingerprint: Fingerprint,
}

impl PhysicalExprKey {
    pub fn stable_fingerprint(&self) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_u64(self.implementation.0 as u64);
        builder.write_u64(self.logical.0 as u64);
        builder.write_fingerprint(self.payload_fingerprint);
        builder.write_u64(self.children.len() as u64);
        for child in &self.children {
            builder.write_u64(child.0 as u64);
        }
        builder.finish()
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalExpr {
    pub id: PhysicalExprId,
    pub key: PhysicalExprKey,
    pub payload: PhysicalPayloadId,
    pub provided: ProvidedProperties,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowGoal {
    All,
    AtMost(u64),
}

impl RowGoal {
    pub(crate) const fn stable_tag(self) -> u64 {
        match self {
            Self::All => 0,
            Self::AtMost(rows) => rows.saturating_add(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GrantGoalKey {
    Invariant(AdmissibleGrantSetId),
    Class(ResourceGrantClassId),
}

impl GrantGoalKey {
    pub(crate) const fn stable_tag(self) -> u64 {
        match self {
            Self::Invariant(set) => set.0 as u64,
            Self::Class(class) => (1_u64 << 63) | class.0 as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OptimizationGoal {
    pub required: PropertySetId,
    pub row_goal: RowGoal,
    pub objective: ObjectiveProfileId,
    pub grant: GrantGoalKey,
    pub context: OptimizationContextId,
}

/// Canonical execution context for a goal.
///
/// Region membership is expression-path state, not a property of a semantic
/// group: one group may contain both a sharing owner and an equivalent inline
/// expression.  Interning the active required facets here lets those
/// expressions derive different child goals without cloning semantic groups
/// or assigning one global region membership to every occurrence.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct OptimizationContext {
    required_region_facets: Box<[Fingerprint]>,
}

impl OptimizationContext {
    pub fn new(required_region_facets: impl IntoIterator<Item = Fingerprint>) -> Self {
        let mut required_region_facets = required_region_facets.into_iter().collect::<Vec<_>>();
        required_region_facets.sort_unstable();
        required_region_facets.dedup();
        Self {
            required_region_facets: required_region_facets.into_boxed_slice(),
        }
    }

    pub fn required_region_facets(&self) -> &[Fingerprint] {
        &self.required_region_facets
    }
}

#[derive(Debug, Clone)]
pub struct Winner {
    pub expression: PhysicalExprId,
    pub child_goals: Box<[(GroupId, OptimizationGoal)]>,
    pub enforcers: Box<[EnforcerStep]>,
    pub enforcer_cost_input: super::engine::EnforcerCostInput,
    pub provided: ProvidedProperties,
    /// Operator-local cost retained so WinnerVerifier can independently
    /// replay composition instead of trusting the enumerator's total.
    pub local_cost: SearchCost,
    pub source_filter_apply_cost: Option<SearchCost>,
    pub cost_composition: CostComposition,
    pub cost: SearchCost,
    /// Disjoint base-source work retained for safe non-local selectivity
    /// composition. This evidence is replayed with the winner tree and is not
    /// embedded in the fixed-size hot SearchCost value.
    pub source_work: Box<[super::rules::SourceWork]>,
    pub physical_fingerprint: Fingerprint,
    pub joint_cost_proof: Option<JointCostProof>,
}

#[derive(Debug, Clone)]
pub struct WinnerFrontier {
    candidates: Vec<Winner>,
    limit: usize,
}

impl WinnerFrontier {
    fn new(limit: u8) -> Self {
        Self {
            candidates: Vec::new(),
            limit: usize::from(limit.max(1)),
        }
    }

    pub fn selected(&self) -> Option<&Winner> {
        self.candidates.first()
    }

    pub fn candidates(&self) -> &[Winner] {
        &self.candidates
    }

    /// Retain the bounded non-dominated set, then order it by the explicit
    /// objective and deterministic Memo insertion rank.  The rank keeps the
    /// mandatory baseline ahead of cost-identical optional alternatives; the
    /// fingerprint only distinguishes recipes for the same physical
    /// expression.  This prevents catalog object IDs and query-local carrier
    /// IDs embedded in a plan fingerprint from changing an exact-tie winner
    /// across cold compilations.
    fn insert(&mut self, goal: OptimizationGoal, winner: Winner) -> bool {
        let old_selected = self.selected().map(|entry| entry.physical_fingerprint);

        if self.candidates.iter().any(|incumbent| {
            incumbent.cost.dominates(&winner.cost)
                || (costs_equal(&incumbent.cost, &winner.cost)
                    && winner_tie_break(incumbent) <= winner_tie_break(&winner))
        }) {
            return false;
        }

        self.candidates.retain(|incumbent| {
            !(winner.cost.dominates(&incumbent.cost)
                || costs_equal(&winner.cost, &incumbent.cost)
                    && winner_tie_break(&winner) < winner_tie_break(incumbent))
        });
        self.candidates.push(winner);
        self.candidates.sort_by(|left, right| {
            compare_objective(left, right, goal.objective)
                .then_with(|| winner_tie_break(left).cmp(&winner_tie_break(right)))
        });
        self.candidates.truncate(self.limit);

        old_selected != self.selected().map(|entry| entry.physical_fingerprint)
    }
}

fn winner_tie_break(winner: &Winner) -> (PhysicalExprId, Fingerprint) {
    (winner.expression, winner.physical_fingerprint)
}

fn costs_equal(left: &SearchCost, right: &SearchCost) -> bool {
    left == right
}

fn compare_objective(
    left: &Winner,
    right: &Winner,
    objective: ObjectiveProfileId,
) -> std::cmp::Ordering {
    // Objective IDs are registry identities. The built-in profiles reserve
    // 0=latency, 1=throughput, 2=memory, 3=robustness. Feasibility and memory
    // completion are compared before every soft objective. Latency ranks the
    // expected critical path; uncertainty remains a deterministic tie-break
    // and is the primary quantity for robustness. Unknown extension profiles
    // fail closed to conservative risk ordering until their registry owns the
    // comparison contract.
    left.cost
        .memory_completion
        .preference_cmp(right.cost.memory_completion)
        .then_with(|| match objective.0 {
            1 => left.cost.resources_expected[0]
                .total_cmp(&right.cost.resources_expected[0])
                .then_with(|| {
                    left.cost
                        .score
                        .risk_adjusted
                        .total_cmp(&right.cost.score.risk_adjusted)
                })
                .then_with(|| {
                    left.cost
                        .peak_memory_upper
                        .cmp(&right.cost.peak_memory_upper)
                }),
            2 => left
                .cost
                .peak_memory_upper
                .cmp(&right.cost.peak_memory_upper)
                .then_with(|| {
                    left.cost
                        .score
                        .risk_adjusted
                        .total_cmp(&right.cost.score.risk_adjusted)
                })
                .then_with(|| {
                    left.cost
                        .spill_bytes_expected
                        .cmp(&right.cost.spill_bytes_expected)
                }),
            3 => left
                .cost
                .score
                .range
                .upper
                .total_cmp(&right.cost.score.range.upper)
                .then_with(|| {
                    left.cost
                        .score
                        .risk_adjusted
                        .total_cmp(&right.cost.score.risk_adjusted)
                })
                .then_with(|| {
                    left.cost
                        .peak_memory_upper
                        .cmp(&right.cost.peak_memory_upper)
                }),
            0 => left
                .cost
                .critical_path
                .expected
                .total_cmp(&right.cost.critical_path.expected)
                .then_with(|| {
                    left.cost
                        .score
                        .range
                        .expected
                        .total_cmp(&right.cost.score.range.expected)
                })
                .then_with(|| {
                    left.cost
                        .score
                        .risk_adjusted
                        .total_cmp(&right.cost.score.risk_adjusted)
                })
                .then_with(|| {
                    left.cost
                        .peak_memory_upper
                        .cmp(&right.cost.peak_memory_upper)
                }),
            _ => left
                .cost
                .score
                .risk_adjusted
                .total_cmp(&right.cost.score.risk_adjusted)
                .then_with(|| {
                    left.cost
                        .score
                        .range
                        .upper
                        .total_cmp(&right.cost.score.range.upper)
                })
                .then_with(|| {
                    left.cost
                        .peak_memory_upper
                        .cmp(&right.cost.peak_memory_upper)
                }),
        })
}

#[derive(Debug)]
pub struct Group {
    pub id: GroupId,
    pub schema: GroupSchema,
    pub logical_properties: LogicalProperties,
    pub cardinality: GroupCardinality,
    logical_exprs: Vec<LogicalExprId>,
    /// Last Memo-global revision that changed the logical expression set.
    /// Transformation consumers use it to distinguish a completed match from
    /// one whose child frontier has since changed, including through rollback.
    logical_expression_version: u64,
    physical_exprs: Vec<PhysicalExprId>,
    logical_index: BTreeMap<LogicalExprKey, LogicalExprId>,
    physical_index: BTreeMap<PhysicalExprKey, PhysicalExprId>,
    winner_frontiers: BTreeMap<OptimizationGoal, WinnerFrontier>,
    pub ledger: SearchLedger,
}

impl Group {
    pub fn logical_exprs(&self) -> &[LogicalExprId] {
        &self.logical_exprs
    }

    pub fn logical_expression_version(&self) -> u64 {
        self.logical_expression_version
    }

    pub fn physical_exprs(&self) -> &[PhysicalExprId] {
        &self.physical_exprs
    }

    pub fn winner(&self, goal: OptimizationGoal) -> Option<&Winner> {
        self.winner_frontiers.get(&goal)?.selected()
    }

    pub fn winners(&self) -> impl Iterator<Item = (&OptimizationGoal, &Winner)> {
        self.winner_frontiers
            .iter()
            .filter_map(|(goal, frontier)| frontier.selected().map(|winner| (goal, winner)))
    }

    pub fn winner_frontier(&self, goal: OptimizationGoal) -> Option<&WinnerFrontier> {
        self.winner_frontiers.get(&goal)
    }

    pub fn winner_frontiers(&self) -> impl Iterator<Item = (&OptimizationGoal, &WinnerFrontier)> {
        self.winner_frontiers.iter()
    }
}

#[derive(Debug)]
pub struct Memo {
    groups: Vec<Group>,
    parents: Vec<GroupId>,
    logical_exprs: Vec<LogicalExpr>,
    physical_exprs: Vec<PhysicalExpr>,
    logical_owners: Vec<GroupId>,
    physical_owners: Vec<GroupId>,
    properties: PropertyInterner,
    optimization_contexts: Vec<OptimizationContext>,
    optimization_context_index: BTreeMap<OptimizationContext, OptimizationContextId>,
    optimization_contexts_frozen: bool,
    logical_frontier_revision: u64,
    budget: SearchBudget,
    calibration: Arc<MachineCalibrationBundle>,
    regions: RegionForest,
}

#[derive(Debug)]
pub(crate) struct TransformationSavepoint {
    group_count: usize,
    logical_expression_count: usize,
    regions: RegionForest,
}

impl Memo {
    pub fn new(budget: SearchBudget) -> Self {
        let root_context = OptimizationContext::default();
        Self {
            groups: Vec::new(),
            parents: Vec::new(),
            logical_exprs: Vec::new(),
            physical_exprs: Vec::new(),
            logical_owners: Vec::new(),
            physical_owners: Vec::new(),
            properties: PropertyInterner::default(),
            optimization_contexts: vec![root_context.clone()],
            optimization_context_index: BTreeMap::from([(
                root_context,
                OptimizationContextId::new(0),
            )]),
            optimization_contexts_frozen: false,
            logical_frontier_revision: 0,
            budget,
            calibration: Arc::new(MachineCalibrationBundle::default()),
            regions: RegionForest::default(),
        }
    }

    pub fn set_calibration(&mut self, calibration: Arc<MachineCalibrationBundle>) {
        self.calibration = calibration;
    }

    pub fn calibration(&self) -> &MachineCalibrationBundle {
        self.calibration.as_ref()
    }

    pub fn set_regions(&mut self, regions: RegionForest) {
        self.regions = regions;
    }

    /// Capture the append-only relational state available to a transformation.
    /// Physical expressions, winners, and property sets are not writable in
    /// this search phase and therefore are intentionally absent.
    pub(crate) fn transformation_savepoint(&self) -> TransformationSavepoint {
        TransformationSavepoint {
            group_count: self.groups.len(),
            logical_expression_count: self.logical_exprs.len(),
            regions: self.regions.clone(),
        }
    }

    pub(crate) fn rollback_transformation(
        &mut self,
        savepoint: TransformationSavepoint,
    ) -> Result<()> {
        if savepoint.group_count > self.groups.len()
            || savepoint.logical_expression_count > self.logical_exprs.len()
            || savepoint.logical_expression_count > self.logical_owners.len()
        {
            return Err(paro_error::internal(
                "transformation rollback exceeds the current Memo generation",
            ));
        }
        for index in (savepoint.logical_expression_count..self.logical_exprs.len()).rev() {
            let id = LogicalExprId::new(index);
            let owner = self.logical_owners[index];
            if owner.index() >= self.groups.len() {
                return Err(paro_error::internal(
                    "transformation rollback found an invalid logical owner",
                ));
            }
            // Expressions owned by appended groups disappear with the group.
            // An expression appended to an existing target group must also be
            // removed from both of that group's indexes. This makes rollback
            // complete by construction instead of relying on a debug-only
            // append-ownership assertion.
            if owner.index() < savepoint.group_count {
                let expression = &self.logical_exprs[index];
                self.logical_frontier_revision = self
                    .logical_frontier_revision
                    .checked_add(1)
                    .ok_or_else(|| paro_error::internal("Memo frontier revision overflow"))?;
                let group = &mut self.groups[owner.index()];
                if group.logical_exprs.pop() != Some(id)
                    || group.logical_index.remove(&expression.key) != Some(id)
                {
                    return Err(paro_error::internal(
                        "transformation rollback found inconsistent group membership",
                    ));
                }
                group.logical_expression_version = self.logical_frontier_revision;
            }
        }
        self.logical_exprs
            .truncate(savepoint.logical_expression_count);
        self.logical_owners
            .truncate(savepoint.logical_expression_count);
        self.groups.truncate(savepoint.group_count);
        self.parents.truncate(savepoint.group_count);
        self.regions = savepoint.regions;
        Ok(())
    }

    pub(crate) fn appended_groups_since(
        &self,
        savepoint: &TransformationSavepoint,
    ) -> Result<Box<[GroupId]>> {
        if savepoint.group_count > self.groups.len() {
            return Err(paro_error::internal(
                "transformation commit exceeds the current Memo generation",
            ));
        }
        Ok((savepoint.group_count..self.groups.len())
            .map(GroupId::new)
            .filter(|group| self.canonical_group(*group) == *group)
            .collect::<Vec<_>>()
            .into_boxed_slice())
    }

    pub fn regions(&self) -> &RegionForest {
        &self.regions
    }

    /// Add a facet, or extend an existing facet, while transformations are
    /// still in the logical exploration phase. Region ids may be reassigned
    /// by normalization, so callers attach implementations by stable facet
    /// fingerprint and must invoke this before physical recipes are built.
    pub fn upsert_region_facet(
        &mut self,
        mut facet: RegionFacet,
    ) -> Result<Box<[super::ids::Fingerprint]>> {
        facet.scope = facet
            .scope
            .iter()
            .map(|group| self.canonical_group(*group))
            .collect();
        let mut facets = self
            .regions
            .nodes
            .iter()
            .flat_map(|region| region.facets.iter().cloned())
            .map(|facet| (facet.fingerprint, facet))
            .collect::<BTreeMap<_, _>>();
        match facets.get_mut(&facet.fingerprint) {
            Some(existing) => {
                if existing.kind != facet.kind
                    || existing.criticality != facet.criticality
                    || existing.scope_contract != facet.scope_contract
                {
                    return Err(paro_error::internal(
                        "planning facet fingerprint changed its contract",
                    ));
                }
                // Priority is a scheduling hint, not facet identity. The
                // fingerprint intentionally excludes it, so equivalent
                // expressions that rediscover the same capability merge at
                // the strongest (smallest) priority instead of becoming an
                // order-dependent contract error.
                existing.priority = existing.priority.min(facet.priority);
                existing.scope.extend(facet.scope);
            }
            None => {
                facets.insert(facet.fingerprint, facet);
            }
        }
        let previously_dropped = self
            .regions
            .dropped_optional_facets
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut regions = RegionForest::normalize(
            facets.into_values(),
            usize::from(self.budget.max_composite_region_groups),
            self.budget.max_mandatory_region_groups as usize,
        )?;
        let dropped = previously_dropped
            .into_iter()
            .chain(regions.dropped_optional_facets.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        regions.dropped_optional_facets = dropped.clone();
        self.regions = regions;
        Ok(dropped)
    }

    pub fn create_group(
        &mut self,
        schema: GroupSchema,
        logical_properties: LogicalProperties,
        cardinality: GroupCardinality,
    ) -> GroupId {
        let id = GroupId::new(self.groups.len());
        self.groups.push(Group {
            id,
            schema,
            logical_properties,
            cardinality,
            logical_exprs: Vec::new(),
            logical_expression_version: 0,
            physical_exprs: Vec::new(),
            logical_index: BTreeMap::new(),
            physical_index: BTreeMap::new(),
            winner_frontiers: BTreeMap::new(),
            ledger: SearchLedger::new(self.budget.clone()),
        });
        self.parents.push(id);
        id
    }

    pub fn canonical_group(&self, mut id: GroupId) -> GroupId {
        loop {
            let parent = self.parents[id.index()];
            if parent == id {
                return id;
            }
            id = parent;
        }
    }

    pub fn group(&self, id: GroupId) -> Option<&Group> {
        self.groups.get(self.canonical_group(id).index())
    }

    pub fn group_mut(&mut self, id: GroupId) -> Option<&mut Group> {
        let id = self.canonical_group(id);
        self.groups.get_mut(id.index())
    }

    /// Resolve a group's canonical cardinality recipe and clamp it by every
    /// hard relational bound along a row-preserving dependency chain.
    pub fn cardinality_envelope(&self, id: GroupId) -> Option<CardinalityEnvelope> {
        fn resolve(
            memo: &Memo,
            id: GroupId,
            visiting: &mut BTreeSet<GroupId>,
        ) -> Option<CardinalityEnvelope> {
            let id = memo.canonical_group(id);
            if !visiting.insert(id) {
                return None;
            }
            let group = memo.group(id)?;
            let mut envelope = group.cardinality.range;
            for input in &group.cardinality.inputs {
                if let Some(input) = resolve(memo, *input, visiting) {
                    envelope = Some(match envelope {
                        Some(current) => current.hull(input),
                        None => input,
                    });
                }
            }
            visiting.remove(&id);
            envelope.map(|range| range.clamp(group.logical_properties.maximum_cardinality))
        }

        resolve(self, id, &mut BTreeSet::new())
    }

    pub fn cardinality_estimate(&self, id: GroupId) -> Option<(u64, u64, u64)> {
        let range = self.cardinality_envelope(id)?;
        let expected = range
            .expected_lower
            .saturating_add(range.expected_upper.saturating_sub(range.expected_lower) / 2);
        Some((range.lower, expected, range.upper))
    }

    pub fn logical_expr(&self, id: LogicalExprId) -> Option<&LogicalExpr> {
        self.logical_exprs.get(id.index())
    }

    pub(crate) fn logical_expr_for_key(
        &self,
        group: GroupId,
        key: &LogicalExprKey,
    ) -> Option<&LogicalExpr> {
        let group = self.canonical_group(group);
        let expression = self.groups.get(group.index())?.logical_index.get(key)?;
        self.logical_expr(*expression)
    }

    pub fn logical_owner(&self, id: LogicalExprId) -> Option<GroupId> {
        self.logical_owners
            .get(id.index())
            .copied()
            .map(|group| self.canonical_group(group))
    }

    pub fn physical_expr(&self, id: PhysicalExprId) -> Option<&PhysicalExpr> {
        self.physical_exprs.get(id.index())
    }

    pub fn physical_owner(&self, id: PhysicalExprId) -> Option<GroupId> {
        self.physical_owners
            .get(id.index())
            .copied()
            .map(|group| self.canonical_group(group))
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn canonical_group_count(&self) -> usize {
        self.groups().count()
    }

    pub fn logical_expr_count(&self) -> usize {
        self.logical_exprs.len()
    }

    pub fn physical_expr_count(&self) -> usize {
        self.physical_exprs.len()
    }

    pub fn exhaustion_counts(&self) -> BTreeMap<BudgetDimension, u64> {
        let mut counts = BTreeMap::new();
        for group in self.groups() {
            for (dimension, _) in group.ledger.exhaustion_events() {
                *counts.entry(*dimension).or_default() += 1;
            }
        }
        counts
    }

    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.groups
            .iter()
            .filter(|group| self.canonical_group(group.id) == group.id)
    }

    pub fn budget(&self) -> &SearchBudget {
        &self.budget
    }

    pub fn intern_required(&mut self, properties: RequiredProperties) -> Result<PropertySetId> {
        self.properties.intern_required(properties)
    }

    pub fn required(&self, id: PropertySetId) -> Option<&RequiredProperties> {
        self.properties.required(id)
    }

    /// Intern expression-path state while the initial logical forest is bound.
    /// Once binding is sealed, transformations can only propagate these IDs.
    pub(super) fn intern_optimization_context(
        &mut self,
        context: OptimizationContext,
    ) -> Result<OptimizationContextId> {
        if self.optimization_contexts_frozen {
            return Err(paro_error::internal(
                "optimization contexts are immutable after initial Memo binding",
            ));
        }
        if let Some(id) = self.optimization_context_index.get(&context) {
            return Ok(*id);
        }
        let id = OptimizationContextId::new(self.optimization_contexts.len());
        self.optimization_contexts.push(context.clone());
        self.optimization_context_index.insert(context, id);
        Ok(id)
    }

    /// Seal the expression-path context catalog before optional search starts.
    ///
    /// Each initial logical expression contributes at most an input and child
    /// context. This proof makes context cardinality linear in the already
    /// admitted logical forest; optional rules cannot form a facet powerset.
    pub(super) fn freeze_optimization_contexts(&mut self) -> Result<()> {
        let linear_bound = self.logical_exprs.len().saturating_mul(2).saturating_add(1);
        if self.optimization_contexts.len() > linear_bound {
            return Err(paro_error::internal(format!(
                "initial optimization context catalog exceeds its linear bound: contexts={}, logical_expressions={}",
                self.optimization_contexts.len(),
                self.logical_exprs.len(),
            )));
        }
        self.optimization_contexts_frozen = true;
        Ok(())
    }

    pub fn optimization_context(&self, id: OptimizationContextId) -> Option<&OptimizationContext> {
        self.optimization_contexts.get(id.index())
    }

    pub fn insert_logical(
        &mut self,
        target: GroupId,
        mut key: LogicalExprKey,
        payload: LogicalPayloadId,
        proof: EquivalenceProof,
    ) -> Result<LogicalExprId> {
        let target = self.canonical_group(target);
        for child in key.children.iter_mut() {
            if child.index() >= self.groups.len() {
                return Err(paro_error::internal(
                    "logical expression references unknown group",
                ));
            }
            *child = self.canonical_group(*child);
        }
        if !matches!(proof, EquivalenceProof::Initial)
            && self.groups[target.index()].logical_exprs.is_empty()
        {
            return Err(paro_error::internal(
                "a non-initial equivalence proof cannot seed an empty group",
            ));
        }
        if let Some(existing) = self.groups[target.index()].logical_index.get(&key).copied() {
            self.logical_exprs[existing.index()].proofs.insert(proof);
            return Ok(existing);
        }
        if matches!(proof, EquivalenceProof::Initial)
            && !self.groups[target.index()].logical_exprs.is_empty()
        {
            return Err(paro_error::internal(
                "Initial proof may only seed a newly-created Memo group",
            ));
        }
        let id = LogicalExprId::new(self.logical_exprs.len());
        self.logical_exprs.push(LogicalExpr {
            id,
            key: key.clone(),
            payload,
            proofs: [proof].into_iter().collect(),
            applied_rules: BTreeSet::new(),
        });
        self.logical_owners.push(target);
        self.logical_frontier_revision = self
            .logical_frontier_revision
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("Memo frontier revision overflow"))?;
        let group = &mut self.groups[target.index()];
        group.logical_index.insert(key, id);
        group.logical_exprs.push(id);
        group.logical_expression_version = self.logical_frontier_revision;
        Ok(id)
    }

    pub fn mark_rule_applied(&mut self, expression: LogicalExprId, rule: RuleId) -> Result<bool> {
        let expression = self
            .logical_exprs
            .get_mut(expression.index())
            .ok_or_else(|| paro_error::internal("unknown logical expression"))?;
        Ok(expression.applied_rules.insert(rule))
    }

    pub fn add_equivalence_proof(
        &mut self,
        expression: LogicalExprId,
        proof: EquivalenceProof,
    ) -> Result<()> {
        if matches!(proof, EquivalenceProof::Initial) {
            return Err(paro_error::internal(
                "Initial is a seed marker, not an equivalence certificate",
            ));
        }
        let expression = self
            .logical_exprs
            .get_mut(expression.index())
            .ok_or_else(|| paro_error::internal("equivalence proof references unknown expr"))?;
        expression.proofs.insert(proof);
        Ok(())
    }

    pub fn insert_physical(
        &mut self,
        target: GroupId,
        mut key: PhysicalExprKey,
        payload: PhysicalPayloadId,
        provided: ProvidedProperties,
    ) -> Result<PhysicalExprId> {
        provided.validate()?;
        let target = self.canonical_group(target);
        if self.logical_expr(key.logical).is_none() {
            return Err(paro_error::internal(
                "physical expression references unknown logical expression",
            ));
        }
        if self.logical_owner(key.logical) != Some(target) {
            return Err(paro_error::internal(
                "physical expression must implement a logical expression in its target group",
            ));
        }
        for child in key.children.iter_mut() {
            if child.index() >= self.groups.len() {
                return Err(paro_error::internal(
                    "physical expression references unknown group",
                ));
            }
            *child = self.canonical_group(*child);
        }
        if let Some(existing) = self.groups[target.index()].physical_index.get(&key) {
            return Ok(*existing);
        }
        let id = PhysicalExprId::new(self.physical_exprs.len());
        self.physical_exprs.push(PhysicalExpr {
            id,
            key: key.clone(),
            payload,
            provided,
        });
        self.physical_owners.push(target);
        let group = &mut self.groups[target.index()];
        group.physical_index.insert(key, id);
        group.physical_exprs.push(id);
        Ok(id)
    }

    pub fn record_winner(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        winner: Winner,
    ) -> Result<bool> {
        winner.local_cost.validate()?;
        winner.cost.validate()?;
        let required = self
            .required(goal.required)
            .ok_or_else(|| paro_error::internal("winner goal references unknown properties"))?;
        if !winner.provided.satisfies(required) {
            return Err(paro_error::internal(
                "winner does not satisfy its required properties",
            ));
        }
        let physical = self
            .physical_expr(winner.expression)
            .ok_or_else(|| paro_error::internal("winner references unknown physical expression"))?;
        let group = self.canonical_group(group);
        if self.physical_owner(winner.expression) != Some(group) {
            return Err(paro_error::internal(
                "winner physical expression belongs to a different group",
            ));
        }
        let recomputed =
            replay_enforcer_chain(physical.provided.clone(), required, &winner.enforcers)?;
        if recomputed != winner.provided {
            return Err(paro_error::internal(
                "winner provided properties disagree with the recomputed enforcer chain",
            ));
        }
        for (child, child_goal) in winner.child_goals.iter() {
            let Some(child_winner) = self
                .group(*child)
                .and_then(|group| group.winner(*child_goal))
            else {
                return Err(paro_error::internal(
                    "winner contains an unresolved or invalid child goal",
                ));
            };
            child_winner.cost.validate()?;
        }
        let recomputed = recompute_winner_cost(self, &winner)?;
        if recomputed.cost != winner.cost || recomputed.source_work != winner.source_work {
            return Err(paro_error::internal(
                "winner cumulative cost or source-work evidence disagrees with composition",
            ));
        }

        let frontier_limit = self.budget.max_pareto_winners_per_goal;
        let slot = self.groups[group.index()].winner_frontiers.entry(goal);
        match slot {
            std::collections::btree_map::Entry::Vacant(entry) => {
                let mut frontier = WinnerFrontier::new(frontier_limit);
                frontier.insert(goal, winner);
                entry.insert(frontier);
                Ok(true)
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                Ok(entry.get_mut().insert(goal, winner))
            }
        }
    }

    pub fn merge_groups(&mut self, left: GroupId, right: GroupId) -> Result<GroupId> {
        let left = self.canonical_group(left);
        let right = self.canonical_group(right);
        if left == right {
            return Ok(left);
        }
        let (canonical, secondary) = if left < right {
            (left, right)
        } else {
            (right, left)
        };
        let canonical_facts = &self.groups[canonical.index()].logical_properties;
        let secondary_facts = &self.groups[secondary.index()].logical_properties;
        if self.groups[canonical.index()].schema != self.groups[secondary.index()].schema
            || canonical_facts.unique_keys != secondary_facts.unique_keys
            || canonical_facts.outer_references != secondary_facts.outer_references
        {
            return Err(paro_error::internal(format!(
                "cannot merge Memo groups with different output contracts or logical facts: \
                 left={canonical:?} schema={:?} facts={:?}; \
                 right={secondary:?} schema={:?} facts={:?}",
                self.groups[canonical.index()].schema,
                self.groups[canonical.index()].logical_properties,
                self.groups[secondary.index()].schema,
                self.groups[secondary.index()].logical_properties,
            )));
        }
        self.parents[secondary.index()] = canonical;
        self.logical_frontier_revision = self
            .logical_frontier_revision
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("Memo frontier revision overflow"))?;

        let (canonical_group, secondary_group) =
            two_groups_mut(&mut self.groups, canonical.index(), secondary.index());
        // Equivalent expressions can establish different conservative row
        // bounds (for example, a decorrelated plan can prove a tighter cap
        // than its dependent form). Both proofs describe the same relation,
        // so their intersection is valid for the complete equivalence class.
        canonical_group
            .logical_properties
            .merge_equivalent_facts(&secondary_group.logical_properties);
        canonical_group.cardinality = std::mem::take(&mut canonical_group.cardinality)
            .canonical_with(std::mem::take(&mut secondary_group.cardinality));
        canonical_group.ledger.merge_from(&secondary_group.ledger);
        canonical_group
            .logical_exprs
            .append(&mut secondary_group.logical_exprs);
        canonical_group.logical_expression_version = self.logical_frontier_revision;
        canonical_group
            .physical_exprs
            .append(&mut secondary_group.physical_exprs);
        canonical_group.winner_frontiers.clear();
        secondary_group.winner_frontiers.clear();

        self.recanonicalize_after_merge();
        Ok(canonical)
    }

    fn recanonicalize_after_merge(&mut self) {
        let parents = self.parents.clone();
        let canonical = |mut id: GroupId| loop {
            let parent = parents[id.index()];
            if parent == id {
                return id;
            }
            id = parent;
        };
        for expression in &mut self.logical_exprs {
            for child in expression.key.children.iter_mut() {
                *child = canonical(*child);
            }
        }
        for expression in &mut self.physical_exprs {
            for child in expression.key.children.iter_mut() {
                *child = canonical(*child);
            }
        }
        for group in &mut self.groups {
            group.cardinality.inputs = group
                .cardinality
                .inputs
                .iter()
                .map(|input| canonical(*input))
                .collect();
        }
        for owner in &mut self.logical_owners {
            *owner = canonical(*owner);
        }
        for owner in &mut self.physical_owners {
            *owner = canonical(*owner);
        }
        for group in &mut self.groups {
            group.logical_index.clear();
            group.physical_index.clear();
            group.winner_frontiers.clear();
            group.logical_exprs.sort_unstable();
            group.logical_exprs.dedup_by(|left, right| {
                self.logical_exprs[left.index()].key == self.logical_exprs[right.index()].key
            });
            for &expression in &group.logical_exprs {
                group.logical_index.insert(
                    self.logical_exprs[expression.index()].key.clone(),
                    expression,
                );
            }
            group.physical_exprs.sort_unstable();
            group.physical_exprs.dedup_by(|left, right| {
                self.physical_exprs[left.index()].key == self.physical_exprs[right.index()].key
            });
            for &expression in &group.physical_exprs {
                group.physical_index.insert(
                    self.physical_exprs[expression.index()].key.clone(),
                    expression,
                );
            }
        }
    }
}

fn recompute_winner_cost(memo: &Memo, winner: &Winner) -> Result<super::engine::ComposedCost> {
    let mut child_costs = Vec::with_capacity(winner.child_goals.len());
    let mut child_source_work = Vec::with_capacity(winner.child_goals.len());
    for (child, child_goal) in winner.child_goals.iter().copied() {
        let child_winner = memo
            .group(child)
            .and_then(|group| group.winner(child_goal))
            .ok_or_else(|| paro_error::internal("winner cost replay lost a child winner"))?;
        child_costs.push(child_winner.cost);
        child_source_work.push(child_winner.source_work.as_ref());
    }
    let mut composed = super::engine::compose_candidate_cost_with_sources(
        winner.local_cost,
        winner.source_filter_apply_cost,
        &child_costs,
        &child_source_work,
        winner.cost_composition.clone(),
    )?;
    composed.cost =
        super::engine::constrain_composed_cost_to_grant(composed.cost, winner.enforcer_cost_input)?
            .ok_or_else(|| paro_error::internal("recorded winner exceeds its resource grant"))?;
    let enforcer_cost = super::engine::enforcer_cost(
        &winner.enforcers,
        winner.enforcer_cost_input,
        memo.calibration(),
    )?
    .ok_or_else(|| paro_error::internal("recorded winner has an infeasible enforcer grant"))?;
    composed.cost = super::engine::constrain_composed_cost_to_grant(
        composed.cost.sequential(enforcer_cost)?,
        winner.enforcer_cost_input,
    )?
    .ok_or_else(|| paro_error::internal("recorded winner enforcers exceed its resource grant"))?;
    Ok(composed)
}

fn two_groups_mut(groups: &mut [Group], left: usize, right: usize) -> (&mut Group, &mut Group) {
    assert_ne!(left, right);
    if left < right {
        let (before_right, from_right) = groups.split_at_mut(right);
        (&mut before_right[left], &mut from_right[0])
    } else {
        let (before_left, from_left) = groups.split_at_mut(left);
        (&mut from_left[0], &mut before_left[right])
    }
}

#[cfg(test)]
#[path = "memo/tests.rs"]
mod tests;
