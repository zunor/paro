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
    AdmissibleGrantSetId, CandidateId, Fingerprint, GroupId, ImplementationId, LogicalExprId,
    LogicalPayloadId, OptimizationContextId, PhysicalExprId, PhysicalPayloadId, PropertySetId,
    ResourceGrantClassId, RuleId, StableFingerprintBuilder,
};
use super::properties::{PropertyInterner, ProvidedProperties, RequiredProperties};
use super::region::{JointCostProof, RegionFacet, RegionForest};
use super::rules::CostComposition;
use crate::physical::ObjectiveProfile;
use paro_storage::statistics::{DistinctEvidence, DistinctProvenance};
use std::sync::{Arc, OnceLock};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalProperties {
    pub unique_keys: BTreeSet<Box<[super::ids::ColumnId]>>,
    pub outer_references: BTreeSet<super::ids::ColumnId>,
    pub maximum_cardinality: Option<u64>,
    /// Expression-independent domains keyed by the group's stable ColumnIds.
    /// Physical alternatives and parent transformations consume this shared
    /// fact instead of retaining an expression-local statistics snapshot.
    pub column_domains: BTreeMap<super::ids::ColumnId, GroupColumnDomain>,
    pub column_values:
        BTreeMap<super::ids::ColumnId, paro_planner::operator::bound_reference::BoundColumnValues>,
    /// Positional bridges from CTE scan-local ColumnIds to producer groups.
    /// Equivalent references may originate from different CTE identities, so
    /// this is a canonical set rather than an insertion-order-sensitive slot.
    /// No physical winner or materialized payload is captured here.
    pub cte_references: BTreeSet<CteReferenceDomain>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CteReferenceDomain {
    pub cte_index: usize,
    pub columns: Box<[super::ids::ColumnId]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CteProducerDomain {
    group: GroupId,
    columns: Box<[super::ids::ColumnId]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupColumnDomain {
    /// Hull of observed/estimated NDV evidence for equivalent expressions.
    /// The lower/upper pair is the only conservative contract.  `expected`
    /// below is a deterministic ranking estimate for costing, not a claim
    /// that any one physical alternative observed the midpoint value.
    pub expected_lower: u64,
    pub expected_upper: u64,
    /// Stable costing point retained from the evidence sources. It is not
    /// reconstructed from the uncertainty hull midpoint.
    pub ranking_point: u64,
    /// Predicate/schema proof. Unlike observed HLL state, this remains a safe
    /// upper bound after data changes permitted by the compiled plan.
    pub guaranteed_upper: Option<u64>,
    /// Only a domain made entirely from complete observations may satisfy a
    /// complete-domain runtime-filter proof.
    pub provenance: DistinctProvenance,
}

impl GroupColumnDomain {
    pub fn new(expected: Option<u64>, guaranteed_upper: Option<u64>) -> Option<Self> {
        if expected.is_none() && guaranteed_upper.is_none() {
            return None;
        }
        let expected = expected.unwrap_or(0);
        Some(Self {
            expected_lower: expected,
            expected_upper: expected,
            ranking_point: expected,
            guaranteed_upper,
            provenance: if expected == 0 {
                DistinctProvenance::Unknown
            } else {
                DistinctProvenance::Derived
            },
        })
    }

    /// Construct a domain directly from column evidence.  The ranking point
    /// remains the estimator's point, while lower/upper retain proof bounds;
    /// callers no longer have to infer provenance from a scalar NDV.
    pub fn from_evidence(
        evidence: DistinctEvidence,
        cardinality_maximum: Option<u64>,
    ) -> Option<Self> {
        let evidence = evidence.normalized();
        let point = evidence.point;
        let expected = cardinality_maximum.map_or(point, |rows| point.min(rows));
        let lower = cardinality_maximum.map_or(evidence.lower, |rows| evidence.lower.min(rows));
        let upper = cardinality_maximum
            .or(evidence.upper)
            .unwrap_or(expected)
            .max(lower)
            .max(expected);
        if expected == 0 && lower == 0 && evidence.upper.is_none() && cardinality_maximum.is_none()
        {
            return None;
        }
        Some(Self {
            expected_lower: lower,
            expected_upper: upper,
            ranking_point: expected.clamp(lower, upper),
            guaranteed_upper: cardinality_maximum
                .zip(evidence.upper)
                .map(|(rows, distinct)| rows.min(distinct))
                .or(cardinality_maximum)
                .or(evidence.upper),
            provenance: evidence.provenance,
        })
    }

    pub fn expected(self) -> Option<u64> {
        (self.ranking_point > 0).then_some(self.ranking_point)
    }

    pub(crate) fn canonical_with(self, other: Self) -> Self {
        let ranking_point = match (self.ranking_point, other.ranking_point) {
            (0, point) | (point, 0) => point,
            (left, right) => left.saturating_add(right.saturating_sub(left) / 2),
        };
        Self {
            expected_lower: match (self.expected_lower, other.expected_lower) {
                (0, right) => right,
                (left, 0) => left,
                (left, right) => left.min(right),
            },
            expected_upper: self.expected_upper.max(other.expected_upper),
            guaranteed_upper: match (self.guaranteed_upper, other.guaranteed_upper) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(bound), None) | (None, Some(bound)) => Some(bound),
                (None, None) => None,
            },
            ranking_point,
            provenance: merge_distinct_provenance(self.provenance, other.provenance),
        }
    }
}

fn merge_distinct_provenance(
    left: DistinctProvenance,
    right: DistinctProvenance,
) -> DistinctProvenance {
    use DistinctProvenance::*;
    match (left, right) {
        (ObservedFull, ObservedFull) => ObservedFull,
        (
            ObservedPartial {
                observed_rows,
                total_rows,
            },
            ObservedFull,
        )
        | (
            ObservedFull,
            ObservedPartial {
                observed_rows,
                total_rows,
            },
        ) => ObservedPartial {
            observed_rows,
            total_rows,
        },
        (
            ObservedPartial {
                observed_rows: left_rows,
                total_rows: left_total,
            },
            ObservedPartial {
                observed_rows: right_rows,
                total_rows: right_total,
            },
        ) => ObservedPartial {
            observed_rows: left_rows.min(right_rows),
            total_rows: left_total.max(right_total),
        },
        (ObservedFull, Derived) | (Derived, ObservedFull) | (Derived, Derived) => Derived,
        (ObservedPartial { .. }, Derived)
        | (Derived, ObservedPartial { .. })
        | (ObservedPartial { .. }, Unknown)
        | (Unknown, ObservedPartial { .. }) => Derived,
        (Unknown, other) | (other, Unknown) => other,
    }
}

impl LogicalProperties {
    pub fn same_contract(&self, other: &Self) -> bool {
        self.unique_keys == other.unique_keys && self.outer_references == other.outer_references
    }

    pub fn merge_equivalent_facts(&mut self, other: &Self) -> Result<()> {
        self.maximum_cardinality = match (self.maximum_cardinality, other.maximum_cardinality) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound),
            (None, None) => None,
        };
        for (&column, &domain) in &other.column_domains {
            self.column_domains
                .entry(column)
                .and_modify(|current| *current = current.canonical_with(domain))
                .or_insert(domain);
        }
        self.cte_references
            .extend(other.cte_references.iter().cloned());
        for (column, value) in &other.column_values {
            let value = self
                .column_values
                .get(column)
                .map(|previous| previous.hull(value))
                .transpose()?
                .unwrap_or_else(|| value.clone());
            self.column_values.insert(*column, value);
        }
        Ok(())
    }

    fn stable_fact_fingerprint(&self) -> Fingerprint {
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.memo.logical-facts.v1");
        fingerprint.write_u64(self.unique_keys.len() as u64);
        for key in &self.unique_keys {
            fingerprint.write_u64(key.len() as u64);
            for column in key.iter() {
                fingerprint.write_u64(column.0 as u64);
            }
        }
        fingerprint.write_u64(self.outer_references.len() as u64);
        for column in &self.outer_references {
            fingerprint.write_u64(column.0 as u64);
        }
        fingerprint.write_u64(self.maximum_cardinality.is_some() as u64);
        if let Some(maximum) = self.maximum_cardinality {
            fingerprint.write_u64(maximum);
        }
        fingerprint.write_u64(self.column_domains.len() as u64);
        for (column, domain) in &self.column_domains {
            fingerprint.write_u64(column.0 as u64);
            fingerprint.write_u64(domain.expected_lower);
            fingerprint.write_u64(domain.expected_upper);
            fingerprint.write_u64(domain.ranking_point);
            encode_distinct_provenance(&mut fingerprint, domain.provenance);
            fingerprint.write_u64(domain.guaranteed_upper.is_some() as u64);
            if let Some(upper) = domain.guaranteed_upper {
                fingerprint.write_u64(upper);
            }
        }
        fingerprint.write_u64(self.cte_references.len() as u64);
        fingerprint.write_u64(self.column_values.len() as u64);
        for (column, value) in &self.column_values {
            fingerprint.write_u64(column.0 as u64);
            fingerprint.write_bytes(value.encoding());
        }
        for reference in &self.cte_references {
            fingerprint.write_u64(reference.cte_index as u64);
            fingerprint.write_u64(reference.columns.len() as u64);
            for column in &reference.columns {
                fingerprint.write_u64(column.0 as u64);
            }
        }
        fingerprint.finish()
    }
}

fn encode_distinct_provenance(
    fingerprint: &mut StableFingerprintBuilder,
    provenance: DistinctProvenance,
) {
    match provenance {
        DistinctProvenance::Unknown => fingerprint.write_u64(0),
        DistinctProvenance::Derived => fingerprint.write_u64(1),
        DistinctProvenance::ObservedFull => fingerprint.write_u64(2),
        DistinctProvenance::ObservedPartial {
            observed_rows,
            total_rows,
        } => {
            fingerprint.write_u64(3);
            fingerprint.write_u64(observed_rows);
            fingerprint.write_u64(total_rows);
        }
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
    fn stable_snapshot_fingerprint(&self) -> Fingerprint {
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.memo.statistics-snapshot.v1");
        fingerprint.write_fingerprint(self.recipe);
        fingerprint.write_u64(self.kind as u64);
        fingerprint.write_u64(self.range.is_some() as u64);
        if let Some(range) = self.range {
            fingerprint.write_u64(range.lower);
            fingerprint.write_u64(range.expected_lower);
            fingerprint.write_u64(range.expected_upper);
            fingerprint.write_u64(range.upper);
        }
        fingerprint.write_u64(self.inputs.len() as u64);
        for input in &self.inputs {
            fingerprint.write_u64(input.0 as u64);
        }
        fingerprint.finish()
    }

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
    pub(crate) fn hull(self, other: Self) -> Self {
        Self {
            lower: self.lower.min(other.lower),
            expected_lower: self.expected_lower.min(other.expected_lower),
            expected_upper: self.expected_upper.max(other.expected_upper),
            upper: self.upper.max(other.upper),
        }
    }

    pub(crate) fn clamp(mut self, maximum: Option<u64>) -> Self {
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
    /// Seed expression of a child group created while staging a transformed
    /// root. It establishes provenance without claiming equivalence to an
    /// expression that belongs to the newly-created group.
    TransformationDescendant {
        rule: RuleId,
    },
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
    /// Exact canonical operator encoding. `key.operator` selects a bucket;
    /// these bytes establish equivalence inside it. Generic optimizer-core
    /// tests may omit the encoding and then the complete key is authoritative.
    pub operator_encoding: Option<Arc<[u8]>>,
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
    pub objective: ObjectiveProfile,
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
    filterable_sources: BTreeSet<super::rules::WorkSourceId>,
}

impl OptimizationContext {
    pub fn new(required_region_facets: impl IntoIterator<Item = Fingerprint>) -> Self {
        let mut required_region_facets = required_region_facets.into_iter().collect::<Vec<_>>();
        required_region_facets.sort_unstable();
        required_region_facets.dedup();
        Self {
            required_region_facets: required_region_facets.into_boxed_slice(),
            filterable_sources: BTreeSet::new(),
        }
    }

    pub fn required_region_facets(&self) -> &[Fingerprint] {
        &self.required_region_facets
    }

    pub fn filterable_sources(&self) -> &BTreeSet<super::rules::WorkSourceId> {
        &self.filterable_sources
    }
}

/// Stable reference to the exact child candidate used to cost a parent.
/// Group/goal alone is insufficient because a parent-side source filter can
/// make a non-selected child frontier member globally optimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildWinnerRef {
    pub group: GroupId,
    pub goal: OptimizationGoal,
    /// Immutable winner-arena identity. Frontier pruning, resorting, and group
    /// merging cannot invalidate this reference.
    pub candidate: CandidateId,
}

#[derive(Debug, Clone)]
pub struct Winner {
    /// Assigned exactly once by `Memo::record_winner`.
    pub candidate: CandidateId,
    pub expression: PhysicalExprId,
    pub children: Box<[ChildWinnerRef]>,
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

#[derive(Debug, Clone, Default)]
pub struct WinnerFrontier {
    candidates: Vec<Winner>,
    filterable_sources: BTreeSet<super::rules::WorkSourceId>,
}

#[derive(Debug, Clone, Copy, Default)]
struct FrontierInsertion {
    selected_changed: bool,
    truncated: bool,
}

impl WinnerFrontier {
    pub fn selected(&self) -> Option<&Winner> {
        self.candidates.first()
    }

    pub fn candidates(&self) -> &[Winner] {
        &self.candidates
    }

    /// Retain the complete non-dominated set, then order it by the explicit
    /// objective and deterministic Memo insertion rank.  The rank keeps the
    /// mandatory baseline ahead of cost-identical optional alternatives; the
    /// fingerprint only distinguishes recipes for the same physical
    /// expression.  This prevents catalog object IDs and query-local carrier
    /// IDs embedded in a plan fingerprint from changing an exact-tie winner
    /// across cold compilations.
    #[cfg(test)]
    fn insert(&mut self, goal: OptimizationGoal, winner: Winner) -> bool {
        self.insert_with_limit(goal, winner, usize::MAX)
            .selected_changed
    }

    /// Insert a candidate into the Pareto frontier with an explicit anytime
    /// bound.  The bound is deliberately applied *after* exact dominance and
    /// objective ordering: no candidate is discarded merely because it is
    /// locally more expensive.  If the bounded representation has to evict a
    /// candidate, the caller records a residual search obligation so the
    /// resulting plan cannot claim global closure.
    fn insert_with_limit(
        &mut self,
        goal: OptimizationGoal,
        winner: Winner,
        limit: usize,
    ) -> FrontierInsertion {
        let old_selected = self.selected().map(|entry| entry.physical_fingerprint);

        if self.candidates.iter().any(|incumbent| {
            winner_dominates(incumbent, &winner, &self.filterable_sources)
                || (costs_equal(&incumbent.cost, &winner.cost)
                    && source_response_equal(incumbent, &winner, &self.filterable_sources)
                    && winner_tie_break(incumbent) <= winner_tie_break(&winner))
        }) {
            return FrontierInsertion::default();
        }

        self.candidates.retain(|incumbent| {
            !(winner_dominates(&winner, incumbent, &self.filterable_sources)
                || costs_equal(&winner.cost, &incumbent.cost)
                    && source_response_equal(&winner, incumbent, &self.filterable_sources)
                    && winner_tie_break(&winner) < winner_tie_break(incumbent))
        });
        self.candidates.push(winner);
        self.candidates.sort_by(|left, right| {
            compare_objective(left, right, goal.objective)
                .then_with(|| winner_tie_break(left).cmp(&winner_tie_break(right)))
        });
        let limit = limit.max(1);
        let truncated = self.candidates.len() > limit;
        if truncated {
            self.candidates.truncate(limit);
        }
        FrontierInsertion {
            selected_changed: old_selected
                != self.selected().map(|entry| entry.physical_fingerprint),
            truncated,
        }
    }
}

fn source_response_equal(
    left: &Winner,
    right: &Winner,
    sources: &BTreeSet<super::rules::WorkSourceId>,
) -> bool {
    left.source_work
        .iter()
        .filter(|lane| sources.contains(&lane.source))
        .eq(right
            .source_work
            .iter()
            .filter(|lane| sources.contains(&lane.source)))
}

fn winner_dominates(
    left: &Winner,
    right: &Winner,
    sources: &BTreeSet<super::rules::WorkSourceId>,
) -> bool {
    // A physical goal declares every source an ancestor may filter. Preserve
    // that exact response frontier, but do not retain irrelevant source
    // histories forever across a closed root/sharing boundary.
    source_response_equal(left, right, sources)
        && left.cost.output_pipeline_tasks == right.cost.output_pipeline_tasks
        && left.cost.dominates(&right.cost)
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
    objective: ObjectiveProfile,
) -> std::cmp::Ordering {
    objective.compare(&left.cost, &right.cost)
}

#[derive(Debug)]
pub struct Group {
    pub id: GroupId,
    pub schema: GroupSchema,
    pub logical_properties: LogicalProperties,
    pub cardinality: GroupCardinality,
    /// Canonical fact identities are read far more often than facts change.
    /// A mutable group borrow invalidates both cells conservatively; readers
    /// then serialize each immutable value at most once per mutation epoch.
    logical_fact_fingerprint: OnceLock<Fingerprint>,
    statistics_snapshot_fingerprint: OnceLock<Fingerprint>,
    logical_exprs: Vec<LogicalExprId>,
    /// Last Memo-global revision that changed the logical expression set.
    /// Transformation consumers use it to distinguish a completed match from
    /// one whose child frontier has since changed, including through rollback.
    logical_expression_version: u64,
    physical_exprs: Vec<PhysicalExprId>,
    logical_index: BTreeMap<LogicalExprKey, Vec<LogicalExprId>>,
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

    pub fn logical_fact_fingerprint(&self) -> Fingerprint {
        *self
            .logical_fact_fingerprint
            .get_or_init(|| self.logical_properties.stable_fact_fingerprint())
    }

    pub fn statistics_snapshot_fingerprint(&self) -> Fingerprint {
        *self
            .statistics_snapshot_fingerprint
            .get_or_init(|| self.cardinality.stable_snapshot_fingerprint())
    }

    fn invalidate_fact_fingerprints(&mut self) {
        self.logical_fact_fingerprint.take();
        self.statistics_snapshot_fingerprint.take();
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
    winner_candidates: Vec<WinnerCandidate>,
    properties: PropertyInterner,
    optimization_contexts: Vec<OptimizationContext>,
    optimization_context_index: BTreeMap<OptimizationContext, OptimizationContextId>,
    optimization_contexts_frozen: bool,
    logical_frontier_revision: u64,
    budget: Arc<SearchBudget>,
    calibration: Arc<MachineCalibrationBundle>,
    regions: RegionForest,
    global_ledger: SearchLedger,
    optional_group_budget_sealed: bool,
    cte_producers: BTreeMap<usize, BTreeSet<CteProducerDomain>>,
    changed_cte_domains: BTreeSet<usize>,
    failed_search_obligations: BTreeSet<super::budget::SearchObligation>,
}

#[derive(Debug, Clone)]
struct WinnerCandidate {
    group: GroupId,
    goal: OptimizationGoal,
    winner: Winner,
}

#[derive(Debug)]
pub(crate) struct TransformationSavepoint {
    group_count: usize,
    logical_expression_count: usize,
    regions: RegionForest,
    global_ledger: SearchLedger,
    cte_producers: BTreeMap<usize, BTreeSet<CteProducerDomain>>,
    changed_cte_domains: BTreeSet<usize>,
}

impl Memo {
    pub fn new(budget: SearchBudget) -> Self {
        let root_context = OptimizationContext::default();
        let budget = Arc::new(budget);
        let global_ledger = SearchLedger::new(budget.clone());
        Self {
            groups: Vec::new(),
            parents: Vec::new(),
            logical_exprs: Vec::new(),
            physical_exprs: Vec::new(),
            logical_owners: Vec::new(),
            physical_owners: Vec::new(),
            winner_candidates: Vec::new(),
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
            global_ledger,
            optional_group_budget_sealed: false,
            cte_producers: BTreeMap::new(),
            changed_cte_domains: BTreeSet::new(),
            failed_search_obligations: BTreeSet::new(),
        }
    }

    pub fn set_calibration(&mut self, calibration: Arc<MachineCalibrationBundle>) {
        self.calibration = calibration;
    }

    pub fn calibration(&self) -> &MachineCalibrationBundle {
        self.calibration.as_ref()
    }

    pub(crate) fn register_cte_producer(
        &mut self,
        cte_index: usize,
        group: GroupId,
        columns: Box<[super::ids::ColumnId]>,
    ) {
        let group = self.canonical_group(group);
        if self
            .cte_producers
            .entry(cte_index)
            .or_default()
            .insert(CteProducerDomain { group, columns })
        {
            self.changed_cte_domains.insert(cte_index);
        }
    }

    /// Registry changes are facts too: a previously unresolved CTE reader
    /// must be woken when its first producer is registered.
    pub(crate) fn take_changed_cte_readers(&mut self) -> Vec<GroupId> {
        let changed = std::mem::take(&mut self.changed_cte_domains);
        if changed.is_empty() {
            return Vec::new();
        }
        self.groups()
            .filter(|group| {
                group
                    .logical_properties
                    .cte_references
                    .iter()
                    .any(|reference| changed.contains(&reference.cte_index))
            })
            .map(|group| group.id)
            .collect()
    }

    pub(crate) fn local_statistics_fingerprint(&self, id: GroupId) -> Fingerprint {
        let group = self
            .group(self.canonical_group(id))
            .expect("observed group exists");
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_fingerprint(group.statistics_snapshot_fingerprint());
        for reference in &group.logical_properties.cte_references {
            fingerprint.write_u64(reference.cte_index as u64);
            fingerprint.write_u64(
                self.cte_producers
                    .get(&reference.cte_index)
                    .map_or(0, BTreeSet::len) as u64,
            );
            for producer in self
                .cte_producers
                .get(&reference.cte_index)
                .into_iter()
                .flatten()
            {
                fingerprint.write_u64(self.canonical_group(producer.group).0 as u64);
                fingerprint.write_u64(producer.columns.len() as u64);
                for column in &producer.columns {
                    fingerprint.write_u64(column.0 as u64);
                }
            }
        }
        fingerprint.finish()
    }

    /// Resolve a column domain through the relational group, including a CTE
    /// scan's positional dependency on every equivalent producer expression.
    /// Multiple producer witnesses form an uncertainty hull; none is selected
    /// by a physical winner, so cost search cannot freeze stale payload stats.
    pub(crate) fn column_domain(
        &self,
        group: GroupId,
        column: super::ids::ColumnId,
    ) -> Option<GroupColumnDomain> {
        let group = self.group(self.canonical_group(group))?;
        let direct = group
            .logical_properties
            .column_domains
            .get(&column)
            .copied();
        let producer = group
            .logical_properties
            .cte_references
            .iter()
            .filter_map(|reference| {
                let ordinal = reference
                    .columns
                    .iter()
                    .position(|candidate| *candidate == column)?;
                self.cte_producers
                    .get(&reference.cte_index)?
                    .iter()
                    .filter_map(|producer| {
                        let producer_group = self.group(self.canonical_group(producer.group))?;
                        let producer_column = *producer.columns.get(ordinal)?;
                        producer_group
                            .logical_properties
                            .column_domains
                            .get(&producer_column)
                            .copied()
                    })
                    .reduce(GroupColumnDomain::canonical_with)
            })
            .reduce(GroupColumnDomain::canonical_with);
        producer.or(direct)
    }

    pub(crate) fn column_value_domain(
        &self,
        id: GroupId,
        column: super::ids::ColumnId,
    ) -> Result<Option<paro_planner::operator::bound_reference::BoundColumnValues>> {
        let Some(group) = self.group(self.canonical_group(id)) else {
            return Ok(None);
        };
        let mut values = None;
        for reference in &group.logical_properties.cte_references {
            let Some(ordinal) = reference
                .columns
                .iter()
                .position(|candidate| *candidate == column)
            else {
                continue;
            };
            for producer in self
                .cte_producers
                .get(&reference.cte_index)
                .into_iter()
                .flatten()
            {
                let Some(value) = producer.columns.get(ordinal).and_then(|column| {
                    self.group(producer.group)?
                        .logical_properties
                        .column_values
                        .get(column)
                }) else {
                    continue;
                };
                values = Some(match values {
                    None => value.clone(),
                    Some(previous) => value.hull(&previous)?,
                });
            }
        }
        Ok(values.or_else(|| group.logical_properties.column_values.get(&column).cloned()))
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
            global_ledger: self.global_ledger.clone(),
            cte_producers: self.cte_producers.clone(),
            changed_cte_domains: self.changed_cte_domains.clone(),
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
                if group.logical_exprs.pop() != Some(id) {
                    return Err(paro_error::internal(
                        "transformation rollback found inconsistent group membership",
                    ));
                }
                let remove_bucket = {
                    let bucket = group
                        .logical_index
                        .get_mut(&expression.key)
                        .ok_or_else(|| {
                            paro_error::internal(
                                "transformation rollback lost its logical-expression bucket",
                            )
                        })?;
                    let position = bucket
                        .iter()
                        .position(|candidate| *candidate == id)
                        .ok_or_else(|| {
                            paro_error::internal(
                                "transformation rollback lost its logical-expression identity",
                            )
                        })?;
                    bucket.remove(position);
                    bucket.is_empty()
                };
                if remove_bucket {
                    group.logical_index.remove(&expression.key);
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
        self.global_ledger
            .rollback_to_preserving_exhaustion(savepoint.global_ledger);
        self.cte_producers = savepoint.cte_producers;
        self.changed_cte_domains = savepoint.changed_cte_domains;
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
        facet: RegionFacet,
    ) -> Result<Box<[super::ids::Fingerprint]>> {
        self.upsert_region_facets(std::iter::once(facet))
    }

    /// Apply one staging transaction's complete facet delta and normalize the
    /// forest once. A transformed subtree may publish many runtime-filter and
    /// sharing facets; normalizing after each individual shell turns one Memo
    /// write into a sequence of increasingly expensive full-forest rebuilds.
    pub fn upsert_region_facets(
        &mut self,
        pending: impl IntoIterator<Item = RegionFacet>,
    ) -> Result<Box<[super::ids::Fingerprint]>> {
        let mut facets = self
            .regions
            .nodes
            .iter()
            .flat_map(|region| region.facets.iter().cloned())
            .map(|facet| (facet.fingerprint, facet))
            .collect::<BTreeMap<_, _>>();
        let mut changed = false;
        for mut facet in pending {
            facet.scope = facet
                .scope
                .iter()
                .map(|group| self.canonical_group(*group))
                .collect();
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
                    // expressions that rediscover the same capability merge
                    // at the strongest priority.
                    if existing.priority > facet.priority || !facet.scope.is_subset(&existing.scope)
                    {
                        existing.priority = existing.priority.min(facet.priority);
                        existing.scope.extend(facet.scope);
                        changed = true;
                    }
                }
                None => {
                    facets.insert(facet.fingerprint, facet);
                    changed = true;
                }
            }
        }
        if !changed {
            return Ok(self.regions.dropped_optional_facets.clone());
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
            logical_fact_fingerprint: OnceLock::new(),
            statistics_snapshot_fingerprint: OnceLock::new(),
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

    /// Freeze query-global group envelopes against the immutable initial Memo.
    /// Optional group memory scales with query size; rollback restores these
    /// reservations because discarded groups no longer consume memory. The
    /// separate rule-work ledger retains the cost of discovering them.
    pub(crate) fn seal_optional_group_budget(&mut self) {
        if self.optional_group_budget_sealed {
            return;
        }
        let initial_groups = u32::try_from(self.groups.len().max(1)).unwrap_or(u32::MAX);
        self.global_ledger.set_limit(
            BudgetDimension::Group,
            initial_groups.saturating_mul(self.budget.max_optional_groups_per_initial_group),
        );
        self.global_ledger.set_limit(
            BudgetDimension::CompositionGroup,
            initial_groups.saturating_mul(
                self.budget
                    .max_optional_composition_groups_per_initial_group,
            ),
        );
        self.optional_group_budget_sealed = true;
    }

    /// Create a group owned by optional transformation search. Exhaustion is
    /// an expected incomplete-search result, never an internal error.
    pub(crate) fn create_optional_group(
        &mut self,
        dimension: BudgetDimension,
        allocation_identity: Fingerprint,
        schema: GroupSchema,
        logical_properties: LogicalProperties,
        cardinality: GroupCardinality,
    ) -> Result<Option<GroupId>> {
        self.seal_optional_group_budget();
        let mut event = StableFingerprintBuilder::default();
        event.write_bytes(b"paro.optional-group-allocation.v2");
        event.write_fingerprint(allocation_identity);
        match self.global_ledger.admit_optional(dimension, event.finish()) {
            super::budget::BudgetDecision::Allowed => {}
            super::budget::BudgetDecision::Exhausted => return Ok(None),
            super::budget::BudgetDecision::Duplicate => {
                return Err(paro_error::internal(
                    "optional Memo group allocation identity was reused without reusing its group",
                ));
            }
            super::budget::BudgetDecision::Unconfigured => {
                return Err(paro_error::internal(
                    "optional Memo group allocation reached an unsealed budget",
                ));
            }
        }
        Ok(Some(self.create_group(
            schema,
            logical_properties,
            cardinality,
        )))
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
        let group = self.groups.get_mut(id.index())?;
        group.invalidate_fact_fingerprints();
        Some(group)
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
            let producer_envelope = group
                .logical_properties
                .cte_references
                .iter()
                .flat_map(|reference| {
                    memo.cte_producers
                        .get(&reference.cte_index)
                        .into_iter()
                        .flatten()
                })
                .filter_map(|producer| resolve(memo, producer.group, visiting))
                .reduce(CardinalityEnvelope::hull);
            // A CTE scan observes the current producer relation. Its own
            // payload statistics are only a fallback when the producer has
            // not entered the Memo yet.
            let mut envelope = producer_envelope.or(group.cardinality.range);
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

    pub(crate) fn local_cardinality_envelope(&self, id: GroupId) -> Option<CardinalityEnvelope> {
        self.group(id)?.cardinality.range
    }

    /// Direct evidence dependencies only: callers own traversal, read
    /// tracking, memoization and admission. `true` denotes a CTE producer
    /// whose current range supersedes the scan-local fallback observation.
    pub(crate) fn cardinality_dependencies(
        &self,
        id: GroupId,
    ) -> impl Iterator<Item = (GroupId, bool)> + '_ {
        self.group(id).into_iter().flat_map(move |group| {
            group
                .cardinality
                .inputs
                .iter()
                .copied()
                .map(|group| (group, false))
                .chain(
                    group
                        .logical_properties
                        .cte_references
                        .iter()
                        .flat_map(move |reference| {
                            self.cte_producers
                                .get(&reference.cte_index)
                                .into_iter()
                                .flatten()
                                .map(|producer| (producer.group, true))
                        }),
                )
        })
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
        let expression = self
            .groups
            .get(group.index())?
            .logical_index
            .get(key)?
            .first()?;
        self.logical_expr(*expression)
    }

    pub(crate) fn logical_expr_for_structural_key(
        &self,
        group: GroupId,
        key: &LogicalExprKey,
        operator_encoding: &[u8],
    ) -> Option<&LogicalExpr> {
        let group = self.canonical_group(group);
        self.groups
            .get(group.index())?
            .logical_index
            .get(key)?
            .iter()
            .filter_map(|expression| self.logical_expr(*expression))
            .find(|expression| {
                expression
                    .operator_encoding
                    .as_deref()
                    .is_some_and(|encoding| encoding == operator_encoding)
            })
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
        for (dimension, _) in self.global_ledger.exhaustion_events() {
            *counts.entry(*dimension).or_default() += 1;
        }
        for group in self.groups() {
            for (dimension, _) in group.ledger.exhaustion_events() {
                *counts.entry(*dimension).or_default() += 1;
            }
        }
        counts
    }

    pub fn search_obligations(&self) -> Box<[super::budget::SearchObligation]> {
        use super::budget::{SearchIncompleteReason, SearchObligation};
        let mut obligations = self
            .failed_search_obligations
            .iter()
            .cloned()
            .map(|mut obligation| {
                obligation.group = obligation.group.map(|group| self.canonical_group(group));
                obligation
            })
            .collect::<BTreeSet<_>>();
        obligations.extend(
            self.global_ledger
                .exhaustion_events()
                .map(|(dimension, witness)| SearchObligation {
                    group: None,
                    reason: SearchIncompleteReason::Budget(*dimension),
                    witness: *witness,
                }),
        );
        for group in self.groups() {
            obligations.extend(
                group
                    .ledger
                    .exhaustion_events()
                    .map(|(dimension, witness)| SearchObligation {
                        group: Some(group.id),
                        reason: SearchIncompleteReason::Budget(*dimension),
                        witness: *witness,
                    }),
            );
        }
        obligations.into_iter().collect()
    }

    pub(crate) fn record_failed_rule(
        &mut self,
        group: GroupId,
        rule: RuleId,
        witness: Fingerprint,
        detail: impl Into<Arc<str>>,
    ) {
        self.failed_search_obligations
            .insert(super::budget::SearchObligation {
                group: Some(self.canonical_group(group)),
                reason: super::budget::SearchIncompleteReason::RuleFailure {
                    rule,
                    detail: detail.into(),
                },
                witness,
            });
    }

    pub fn groups(&self) -> impl Iterator<Item = &Group> {
        self.groups
            .iter()
            .filter(|group| self.canonical_group(group.id) == group.id)
    }

    pub fn budget(&self) -> &SearchBudget {
        self.budget.as_ref()
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
        if self.optimization_contexts_frozen {
            return Ok(());
        }
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

    pub fn same_region_context(
        &self,
        left: OptimizationContextId,
        right: OptimizationContextId,
    ) -> bool {
        self.optimization_context(left)
            .zip(self.optimization_context(right))
            .is_some_and(|(left, right)| {
                left.required_region_facets == right.required_region_facets
            })
    }

    /// Refine only physical source demand. Logical transformations cannot
    /// manufacture region scopes after sealing; physical search can request a
    /// different response frontier inside an already-bound region scope.
    pub(super) fn intern_source_demand_context(
        &mut self,
        base: OptimizationContextId,
        sources: BTreeSet<super::rules::WorkSourceId>,
    ) -> Result<OptimizationContextId> {
        let mut context = self
            .optimization_context(base)
            .cloned()
            .ok_or_else(|| paro_error::internal("source demand has no region context"))?;
        context.filterable_sources = sources;
        if let Some(id) = self.optimization_context_index.get(&context) {
            return Ok(*id);
        }
        let id = OptimizationContextId::new(self.optimization_contexts.len());
        self.optimization_contexts.push(context.clone());
        self.optimization_context_index.insert(context, id);
        Ok(id)
    }

    pub fn insert_logical(
        &mut self,
        target: GroupId,
        key: LogicalExprKey,
        payload: LogicalPayloadId,
        proof: EquivalenceProof,
    ) -> Result<LogicalExprId> {
        self.insert_logical_structural(target, key, payload, proof, None)
    }

    pub(crate) fn insert_logical_with_operator_encoding(
        &mut self,
        target: GroupId,
        key: LogicalExprKey,
        payload: LogicalPayloadId,
        proof: EquivalenceProof,
        operator_encoding: Box<[u8]>,
    ) -> Result<LogicalExprId> {
        self.insert_logical_structural(
            target,
            key,
            payload,
            proof,
            Some(Arc::from(operator_encoding)),
        )
    }

    fn insert_logical_structural(
        &mut self,
        target: GroupId,
        mut key: LogicalExprKey,
        payload: LogicalPayloadId,
        proof: EquivalenceProof,
        operator_encoding: Option<Arc<[u8]>>,
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
        if !matches!(
            proof,
            EquivalenceProof::Initial | EquivalenceProof::TransformationDescendant { .. }
        ) && self.groups[target.index()].logical_exprs.is_empty()
        {
            return Err(paro_error::internal(
                "a non-initial equivalence proof cannot seed an empty group",
            ));
        }
        if let Some(existing) = self.groups[target.index()].logical_index.get(&key) {
            let equivalent = existing.iter().copied().find(|existing| {
                self.logical_exprs[existing.index()].operator_encoding == operator_encoding
            });
            if let Some(existing) = equivalent {
                self.logical_exprs[existing.index()].proofs.insert(proof);
                return Ok(existing);
            }
        }
        if matches!(
            proof,
            EquivalenceProof::Initial | EquivalenceProof::TransformationDescendant { .. }
        ) && !self.groups[target.index()].logical_exprs.is_empty()
        {
            return Err(paro_error::internal(
                "seed proof may only initialize a newly-created Memo group",
            ));
        }
        let id = LogicalExprId::new(self.logical_exprs.len());
        self.logical_exprs.push(LogicalExpr {
            id,
            key: key.clone(),
            operator_encoding,
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
        group.logical_index.entry(key).or_default().push(id);
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
        mut winner: Winner,
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
        for child in winner.children.iter() {
            let Some(child_winner) = self.resolve_child_winner(*child) else {
                return Err(paro_error::internal(
                    "winner contains an unresolved or stale child candidate",
                ));
            };
            child_winner.cost.validate()?;
        }
        // Frontier admission is the hot path and receives a cost composed by
        // the engine from exact child candidate references. Replaying that
        // same algebra here would verify every candidate, including ones
        // immediately removed by dominance. WinnerVerifier independently
        // recomposes the bounded retained frontier once search is complete.
        let candidate = CandidateId::new(self.winner_candidates.len());
        winner.candidate = candidate;
        self.winner_candidates.push(WinnerCandidate {
            group,
            goal,
            winner: winner.clone(),
        });
        let filterable_sources = self
            .optimization_context(goal.context)
            .ok_or_else(|| paro_error::internal("winner has no source-demand context"))?
            .filterable_sources
            .clone();
        let frontier_limit = self.budget.max_winner_frontier_candidates_per_goal.max(1) as usize;
        let mut frontier_witness = StableFingerprintBuilder::default();
        frontier_witness.write_bytes(b"paro.winner-frontier-boundary.v1");
        frontier_witness.write_u64(group.0 as u64);
        frontier_witness.write_u64(goal.required.0 as u64);
        frontier_witness.write_u64(goal.row_goal.stable_tag());
        frontier_witness.write_u64(goal.objective.stable_tag());
        frontier_witness.write_u64(goal.grant.stable_tag());
        frontier_witness.write_u64(goal.context.0 as u64);
        frontier_witness.write_fingerprint(winner.physical_fingerprint);
        let frontier_witness = frontier_witness.finish();
        let slot = self.groups[group.index()].winner_frontiers.entry(goal);
        let insertion = match slot {
            std::collections::btree_map::Entry::Vacant(entry) => {
                let mut frontier = WinnerFrontier::default();
                frontier.filterable_sources = filterable_sources;
                let insertion = frontier.insert_with_limit(goal, winner, frontier_limit);
                entry.insert(frontier);
                insertion
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => entry
                .get_mut()
                .insert_with_limit(goal, winner, frontier_limit),
        };
        if insertion.truncated {
            self.groups[group.index()]
                .ledger
                .record_budget_limited(BudgetDimension::WinnerFrontier, frontier_witness);
        }
        Ok(insertion.selected_changed)
    }

    pub fn resolve_child_winner(&self, child: ChildWinnerRef) -> Option<&Winner> {
        let candidate = self.winner_candidates.get(child.candidate.index())?;
        (self.canonical_group(candidate.group) == self.canonical_group(child.group)
            && candidate.goal == child.goal)
            .then_some(&candidate.winner)
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
        canonical_group.invalidate_fact_fingerprints();
        secondary_group.invalidate_fact_fingerprints();
        // Equivalent expressions can establish different conservative row
        // bounds (for example, a decorrelated plan can prove a tighter cap
        // than its dependent form). Both proofs describe the same relation,
        // so their intersection is valid for the complete equivalence class.
        canonical_group
            .logical_properties
            .merge_equivalent_facts(&secondary_group.logical_properties)?;
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
            let mut unique = BTreeMap::<(LogicalExprKey, Option<Arc<[u8]>>), LogicalExprId>::new();
            for expression in std::mem::take(&mut group.logical_exprs) {
                let semantic_key = (
                    self.logical_exprs[expression.index()].key.clone(),
                    self.logical_exprs[expression.index()]
                        .operator_encoding
                        .clone(),
                );
                if let Some(existing) = unique.get(&semantic_key).copied() {
                    let proofs = self.logical_exprs[expression.index()].proofs.clone();
                    self.logical_exprs[existing.index()].proofs.extend(proofs);
                } else {
                    unique.insert(semantic_key, expression);
                    group.logical_exprs.push(expression);
                }
            }
            for &expression in &group.logical_exprs {
                group
                    .logical_index
                    .entry(self.logical_exprs[expression.index()].key.clone())
                    .or_default()
                    .push(expression);
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
