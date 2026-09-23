// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Contextual Memo with expression-local rule history and goal-keyed winners.

use std::cell::Cell;
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
use paro_planner::operator::cte::CteColumnId;
use paro_storage::statistics::{DistinctEvidence, DistinctProvenance};
use std::sync::{Arc, Mutex, OnceLock};

pub(crate) const UNTYPED_LOGICAL_OPERATOR_TAG: u64 = u64::MAX;

mod profile;
pub mod diagnostic_snapshot;
pub use profile::{PhysicalFrontierProfile, PhysicalGroupProfile, PhysicalSearchProfile};

/// A physical costing/scheduling epoch observes one frozen logical fact set.
/// Old exact candidates remain replayable, but cannot seed new fact decisions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CostEpoch(u64);

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
    /// Conflicting advisory types are absorbing unknown evidence. Remembering
    /// this state makes equivalent-fact merge commutative and prevents a later
    /// alternative from reviving a value domain rejected earlier.
    pub conflicting_column_values: BTreeSet<super::ids::ColumnId>,
    /// Definition-column bridges from scan-local columns to producer groups.
    /// Equivalent references may originate from different CTE identities, so
    /// this is a canonical set rather than an insertion-order-sensitive slot.
    /// No physical winner or materialized payload is captured here.
    pub cte_references: BTreeSet<CteReferenceDomain>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CteReferenceDomain {
    pub cte_index: usize,
    pub columns: BTreeMap<CteColumnId, super::ids::ColumnId>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CteProducerDomain {
    group: GroupId,
    columns: BTreeMap<CteColumnId, super::ids::ColumnId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupColumnDomain {
    /// Hull of observed/estimated NDV evidence for equivalent expressions.
    /// The lower/upper pair is the only conservative contract.  `expected`
    /// below is a deterministic ranking estimate for costing, not a claim
    /// that any one physical alternative observed the midpoint value.
    pub expected_lower: u64,
    pub expected_upper: u64,
    /// Largest supplied costing point. This is an existing estimate, never
    /// an average manufactured by alternative insertion order. Keep it
    /// unclamped while composing; apply proof bounds only when reading it.
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
        let point = self
            .guaranteed_upper
            .map_or(self.ranking_point, |upper| self.ranking_point.min(upper));
        (point > 0).then_some(point)
    }

    pub(crate) fn canonical_with(self, other: Self) -> Self {
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
            // NDV evidence forms an idempotent semilattice: rediscovering a
            // derivation cannot cast another vote for a different average.
            // Choosing the largest supplied point is conservative for group
            // state/work ranking, but is not a correctness upper bound.
            ranking_point: self.ranking_point.max(other.ranking_point),
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
        // Complete-domain proof requires every contributing domain to have
        // complete evidence. Missing/derived provenance cannot be restored
        // just by merging a later complete observation.
        (Unknown, _) | (_, Unknown) => Unknown,
        (Derived, _) | (_, Derived) => Derived,
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
        self.conflicting_column_values
            .extend(other.conflicting_column_values.iter().copied());
        for (column, value) in &other.column_values {
            if self.conflicting_column_values.contains(column) {
                continue;
            }
            if self.column_values.get(column).is_some_and(|previous| {
                previous.statistics().get_type() != value.statistics().get_type()
            }) {
                self.conflicting_column_values.insert(*column);
                continue;
            }
            let value = self
                .column_values
                .get(column)
                .map(|previous| previous.hull(value))
                .transpose()?
                .unwrap_or_else(|| value.clone());
            self.column_values.insert(*column, value);
        }
        for column in &self.conflicting_column_values {
            self.column_values.remove(column);
        }
        Ok(())
    }

    fn stable_fact_fingerprint(&self) -> Fingerprint {
        let mut fingerprint = StableFingerprintBuilder::default();
        fingerprint.write_bytes(b"paro.memo.logical-facts.v1");
        fingerprint.write_u64(self.conflicting_column_values.len() as u64);
        for column in &self.conflicting_column_values {
            fingerprint.write_u64(column.0 as u64);
        }
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
            for (definition, column) in &reference.columns {
                fingerprint.write_u64(definition.0 as u64);
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
    /// An estimator consuming an explicit relational constraint. A rewrite
    /// name or equivalence proof alone is not this statistical evidence.
    ConstraintRefined,
    /// A joint estimator over an associative region.
    JoinRegion,
}

/// Canonical, expression-independent cardinality estimate for one Memo group.
///
/// `recipe` identifies relational estimation evidence, not a physical winner.
/// Equivalent alternatives inherit the current group recipe. New relational
/// facts and explicit statistics refreshes own changes to that recipe; the
/// transformation's rule identity is never estimator evidence.
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
        // Input group ordinals are Memo-local and may differ when the same
        // logical plan is rebuilt in an independent Memo. Their semantic
        // facts are tracked as explicit PatternRead dependencies by the
        // callers that resolve inherited cardinality, so only the dependency
        // arity belongs in this local snapshot witness.
        fingerprint.write_u64(self.inputs.len() as u64);
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

/// The single boundary contract used when a planner-produced logical node is
/// lowered into the Memo.
///
/// The key/encoding are the structural identity, while the properties and
/// cardinality are the fact snapshot for this occurrence.  Keeping them in
/// one value makes it impossible for publication to insert one representation
/// and merge a separately reconstructed fact/layout value after the
/// transaction has already committed.  `PlannerLogicalPayload` carries the
/// binding layout, scalar roots and producer/consumer proofs for the same
/// node; this contract carries the Memo-owned portion of that boundary.
#[derive(Debug, Clone)]
pub(crate) struct LogicalInsertionContract {
    pub(crate) target: GroupId,
    pub(crate) key: LogicalExprKey,
    pub(crate) payload: LogicalPayloadId,
    pub(crate) operator_encoding: Option<Box<[u8]>>,
    pub(crate) proof: EquivalenceProof,
    pub(crate) logical_properties: LogicalProperties,
    pub(crate) cardinality: GroupCardinality,
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
    /// Planner-provided semantic operator tag. Core-only Memo users may leave
    /// this absent; the planner uses it only as an exact frontier index key,
    /// never as an equivalence or completeness decision.
    pub operator_tag: Option<u64>,
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
    Parallelism {
        admissible: AdmissibleGrantSetId,
        tasks: u16,
    },
    Class(ResourceGrantClassId),
}

impl GrantGoalKey {
    pub(crate) const fn stable_tag(self) -> u64 {
        match self {
            Self::Invariant(set) => set.0 as u64,
            Self::Parallelism { admissible, tasks } => {
                (1_u64 << 62) | ((tasks as u64) << 32) | admissible.0 as u64
            }
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

/// The phase whose response contract is being requested for a subproblem.
///
/// This is part of the context identity even when two phases currently happen
/// to use the same implementation code. A physical result that is safe to
/// publish during costing is not automatically an executable image, and an
/// execution response must not be reused as a logical discovery result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OptimizationPhase {
    #[default]
    Physical,
    Logical,
    Cost,
    Admission,
    Execution,
}

/// Query-local ownership of a shared artifact. The fingerprint identifies a
/// semantic owner/producer, never a benchmark case or a physical candidate.
/// Keeping it in the context prevents an inline and a shared CTE response from
/// being collapsed merely because their required properties match.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SharedOwnership {
    #[default]
    Private,
    Shared {
        owner: Fingerprint,
    },
    Cte {
        producer: Fingerprint,
    },
}

/// Declares what a caller may consume from a resumable subproblem.
///
/// `Prefix` is intentionally distinct from `Complete`: a useful early
/// frontier cannot be reused as proof that the declared search domain was
/// exhausted. `ParentResponse` retains the exact parent-visible response
/// dimension needed by a composition task.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ContinuationContract {
    #[default]
    Complete,
    Prefix {
        frontier: Fingerprint,
    },
    ParentResponse {
        response: Fingerprint,
    },
}

/// Canonical execution context for a goal.
///
/// Region membership is expression-path state, not a property of a semantic
/// group: one group may contain both a sharing owner and an equivalent inline
/// expression. Interning the active required facets here lets those
/// expressions derive different child goals without cloning semantic groups
/// or assigning one global region membership to every occurrence. The
/// phase/ownership/continuation fields are also structural identity: they
/// prevent demand-driven context reuse from silently dropping a parent-visible
/// response dimension.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct OptimizationContext {
    required_region_facets: Box<[Fingerprint]>,
    filterable_sources: BTreeSet<super::rules::WorkSourceId>,
    phase: OptimizationPhase,
    ownership: SharedOwnership,
    continuation: ContinuationContract,
}

impl OptimizationContext {
    pub fn new(required_region_facets: impl IntoIterator<Item = Fingerprint>) -> Self {
        let mut required_region_facets = required_region_facets.into_iter().collect::<Vec<_>>();
        required_region_facets.sort_unstable();
        required_region_facets.dedup();
        Self {
            required_region_facets: required_region_facets.into_boxed_slice(),
            filterable_sources: BTreeSet::new(),
            phase: OptimizationPhase::default(),
            ownership: SharedOwnership::default(),
            continuation: ContinuationContract::default(),
        }
    }

    pub fn new_with_contract(
        required_region_facets: impl IntoIterator<Item = Fingerprint>,
        phase: OptimizationPhase,
        ownership: SharedOwnership,
        continuation: ContinuationContract,
    ) -> Self {
        let mut context = Self::new(required_region_facets);
        context.phase = phase;
        context.ownership = ownership;
        context.continuation = continuation;
        context
    }

    pub fn with_contract(
        mut self,
        phase: OptimizationPhase,
        ownership: SharedOwnership,
        continuation: ContinuationContract,
    ) -> Self {
        self.phase = phase;
        self.ownership = ownership;
        self.continuation = continuation;
        self
    }

    pub fn required_region_facets(&self) -> &[Fingerprint] {
        &self.required_region_facets
    }

    pub fn filterable_sources(&self) -> &BTreeSet<super::rules::WorkSourceId> {
        &self.filterable_sources
    }

    pub fn phase(&self) -> OptimizationPhase {
        self.phase
    }

    pub fn ownership(&self) -> SharedOwnership {
        self.ownership
    }

    pub fn continuation(&self) -> ContinuationContract {
        self.continuation
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

/// Immutable, self-contained snapshot of one executable winner DAG.
///
/// The Memo remains the owner of the query-wide expression tables and is not
/// copied at a stop boundary.  A frozen candidate instead retains only the
/// exact child choices reachable from the root, the corresponding logical and
/// physical payload handles, and the Winner evidence needed to replay the
/// resource/property/cost contract.  Shared child candidates remain shared
/// through `Arc`, so a multi-consumer plan is not expanded once per parent.
#[derive(Debug, Clone)]
pub struct FrozenCandidate {
    pub reference: ChildWinnerRef,
    pub winner: Arc<Winner>,
    pub logical: Arc<LogicalExpr>,
    pub physical: Arc<PhysicalExpr>,
    pub children: Box<[Arc<FrozenCandidate>]>,
}

/// The complete parent-observable portion of a candidate that is needed for
/// frontier admission.  Child references, enforcer steps and proof/source
/// payloads are deliberately absent: they are only materialized after this
/// summary proves that the candidate can enter the bounded frontier.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CandidateSummary<'a> {
    pub(crate) expression: PhysicalExprId,
    pub(crate) cost: SearchCost,
    pub(crate) source_work: &'a [super::rules::SourceWork],
    pub(crate) physical_fingerprint: Fingerprint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CandidatePreview {
    /// The candidate changes the retained frontier and its owned payload must
    /// be materialized before insertion.
    Publish,
    /// The candidate changes the frontier by retiring an incumbent, but the
    /// bounded position itself is outside the retained prefix.
    MustMaterialize,
    /// An incumbent already dominates the candidate; no frontier mutation is
    /// needed and the owned payload can be skipped. The published candidate
    /// is retained as a monotone dominance witness, so incremental
    /// parent-frontier updates do not have to rescan this proposal. The
    /// witness is valid for the same frozen cost/read context as the summary.
    Rejected { dominator: CandidateId },
    /// The candidate is outside the bounded prefix without retiring an
    /// incumbent.  Record the boundary obligation, but do not persist it.
    Truncated,
}

#[derive(Debug, Clone, Default)]
pub struct WinnerFrontier {
    // The frontier indexes immutable published candidates. Its reordering and
    // pruning must neither copy their proof trees nor retire parent references.
    candidates: Vec<Arc<Winner>>,
    filterable_sources: BTreeSet<super::rules::WorkSourceId>,
    proposals: u64,
    truncations: u64,
    high_water: usize,
}

#[derive(Debug, Default)]
struct FrontierInsertion {
    selected_changed: bool,
    truncated: bool,
    published: Option<Arc<Winner>>,
}

impl WinnerFrontier {
    pub fn selected(&self) -> Option<&Winner> {
        self.candidates.first().map(Arc::as_ref)
    }

    pub fn candidates(&self) -> &[Arc<Winner>] {
        &self.candidates
    }

    /// Check whether a candidate would be published without constructing its
    /// owned child/proof payload.  The comparison is intentionally identical
    /// to `insert_with_limit`: source response, continuation cost and stable
    /// tie-breaks remain visible, so this is not a score-only shortcut.
    pub(crate) fn preview(
        &self,
        goal: OptimizationGoal,
        candidate: CandidateSummary<'_>,
        limit: usize,
    ) -> CandidatePreview {
        if let Some(dominator) = self.candidates.iter().find_map(|incumbent| {
            match winner_continuation_cmp_summary(
                incumbent,
                &candidate,
                &self.filterable_sources,
                goal.objective,
            ) {
                Some(std::cmp::Ordering::Less) => Some(incumbent.candidate),
                Some(std::cmp::Ordering::Equal)
                    if winner_tie_break(incumbent) <= summary_tie_break(&candidate) =>
                {
                    Some(incumbent.candidate)
                }
                _ => None,
            }
        }) {
            return CandidatePreview::Rejected { dominator };
        }

        let mut position = 0usize;
        let mut retires_incumbent = false;
        for incumbent in &self.candidates {
            let removed = match summary_continuation_cmp_winner(
                &candidate,
                incumbent,
                &self.filterable_sources,
                goal.objective,
            ) {
                Some(std::cmp::Ordering::Less) => true,
                Some(std::cmp::Ordering::Equal) => {
                    summary_tie_break(&candidate) >= winner_tie_break(incumbent)
                }
                _ => false,
            };
            if removed {
                retires_incumbent = true;
                continue;
            }
            let order = goal
                .objective
                .compare(&incumbent.cost, &candidate.cost)
                .then_with(|| winner_tie_break(incumbent).cmp(&summary_tie_break(&candidate)));
            if order.is_gt() {
                break;
            }
            position = position.saturating_add(1);
        }
        if position < limit.max(1) {
            CandidatePreview::Publish
        } else if retires_incumbent {
            CandidatePreview::MustMaterialize
        } else {
            CandidatePreview::Truncated
        }
    }

    fn record_rejected_proposal(&mut self) {
        self.proposals = self.proposals.saturating_add(1);
    }

    fn record_truncated_proposal(&mut self) {
        self.proposals = self.proposals.saturating_add(1);
        self.truncations = self.truncations.saturating_add(1);
        self.high_water = self.high_water.max(self.candidates.len().saturating_add(1));
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
        self.proposals = self.proposals.saturating_add(1);
        let old_selected = self.selected().map(|entry| entry.physical_fingerprint);

        if self.candidates.iter().any(|incumbent| {
            match winner_continuation_cmp(
                incumbent,
                &winner,
                &self.filterable_sources,
                goal.objective,
            ) {
                Some(std::cmp::Ordering::Less) => true,
                Some(std::cmp::Ordering::Equal) => {
                    winner_tie_break(incumbent) <= winner_tie_break(&winner)
                }
                _ => false,
            }
        }) {
            return FrontierInsertion::default();
        }

        self.candidates.retain(|incumbent| {
            match winner_continuation_cmp(
                &winner,
                incumbent,
                &self.filterable_sources,
                goal.objective,
            ) {
                Some(std::cmp::Ordering::Less) => false,
                Some(std::cmp::Ordering::Equal) => {
                    winner_tie_break(&winner) >= winner_tie_break(incumbent)
                }
                _ => true,
            }
        });
        // Removal preserves the existing ordering. Insert after exact ties,
        // as a stable full sort would, without sorting the whole frontier for
        // every costed proposal or moving its large inline Winner payloads.
        let position = self.candidates.partition_point(|incumbent| {
            !compare_objective(incumbent, &winner, goal.objective)
                .then_with(|| winner_tie_break(incumbent).cmp(&winner_tie_break(&winner)))
                .is_gt()
        });
        let limit = limit.max(1);
        let truncated = self.candidates.len().saturating_add(1) > limit;
        self.high_water = self.high_water.max(self.candidates.len().saturating_add(1));
        self.truncations = self.truncations.saturating_add(u64::from(truncated));
        let published = (position < limit).then(|| {
            let winner = Arc::new(winner);
            self.candidates.insert(position, Arc::clone(&winner));
            winner
        });
        if truncated {
            self.candidates.truncate(limit);
        }
        FrontierInsertion {
            selected_changed: old_selected
                != self.selected().map(|entry| entry.physical_fingerprint),
            truncated,
            published,
        }
    }
}

fn source_response_equal(
    left: &Winner,
    right: &Winner,
    sources: &BTreeSet<super::rules::WorkSourceId>,
) -> bool {
    if sources.is_empty() {
        return true;
    }
    left.source_work
        .iter()
        .filter(|lane| sources.contains(&lane.source))
        .eq(right
            .source_work
            .iter()
            .filter(|lane| sources.contains(&lane.source)))
}

fn winner_continuation_cmp(
    left: &Winner,
    right: &Winner,
    sources: &BTreeSet<super::rules::WorkSourceId>,
    objective: ObjectiveProfile,
) -> Option<std::cmp::Ordering> {
    // A physical goal declares every source an ancestor may filter. Preserve
    // that exact response frontier, but do not retain irrelevant source
    // histories forever across a closed root/sharing boundary.
    let order = left.cost.continuation_cmp_for(&right.cost, objective)?;
    source_response_equal(left, right, sources).then_some(order)
}

fn winner_continuation_cmp_summary(
    winner: &Winner,
    summary: &CandidateSummary<'_>,
    sources: &BTreeSet<super::rules::WorkSourceId>,
    objective: ObjectiveProfile,
) -> Option<std::cmp::Ordering> {
    let order = winner.cost.continuation_cmp_for(&summary.cost, objective)?;
    let same_sources = if sources.is_empty() {
        true
    } else {
        winner
            .source_work
            .iter()
            .filter(|lane| sources.contains(&lane.source))
            .eq(summary
                .source_work
                .iter()
                .filter(|lane| sources.contains(&lane.source)))
    };
    same_sources.then_some(order)
}

fn summary_continuation_cmp_winner(
    summary: &CandidateSummary<'_>,
    winner: &Winner,
    sources: &BTreeSet<super::rules::WorkSourceId>,
    objective: ObjectiveProfile,
) -> Option<std::cmp::Ordering> {
    let order = summary.cost.continuation_cmp_for(&winner.cost, objective)?;
    let same_sources = if sources.is_empty() {
        true
    } else {
        summary
            .source_work
            .iter()
            .filter(|lane| sources.contains(&lane.source))
            .eq(winner
                .source_work
                .iter()
                .filter(|lane| sources.contains(&lane.source)))
    };
    same_sources.then_some(order)
}

fn summary_tie_break(summary: &CandidateSummary<'_>) -> (PhysicalExprId, Fingerprint) {
    (summary.expression, summary.physical_fingerprint)
}

fn winner_frontier_budget_witness(
    group: GroupId,
    goal: OptimizationGoal,
    physical_fingerprint: Fingerprint,
) -> Fingerprint {
    let mut witness = StableFingerprintBuilder::default();
    witness.write_bytes(b"paro.winner-frontier-boundary.v1");
    witness.write_u64(group.0 as u64);
    witness.write_u64(goal.required.0 as u64);
    witness.write_u64(goal.row_goal.stable_tag());
    witness.write_u64(goal.objective.stable_tag());
    witness.write_u64(goal.grant.stable_tag());
    witness.write_u64(goal.context.0 as u64);
    witness.write_fingerprint(physical_fingerprint);
    witness.finish()
}

fn winner_tie_break(winner: &Winner) -> (PhysicalExprId, Fingerprint) {
    (winner.expression, winner.physical_fingerprint)
}

fn compare_objective(
    left: &Winner,
    right: &Winner,
    objective: ObjectiveProfile,
) -> std::cmp::Ordering {
    objective.compare(&left.cost, &right.cost)
}

#[derive(Debug)]
struct StatisticsReadCache {
    registry_revision: u64,
    /// Values actually read from each lexical producer, in registry order.
    /// Registry structure can stay fixed while one of these values changes.
    producers: Box<[Option<(Fingerprint, Fingerprint)>]>,
    fingerprint: Fingerprint,
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
    /// The read transcript adds definition-to-producer correspondences to
    /// local statistics. Cache its value separately from the registry cursor:
    /// cursor changes cause revalidation, never a new semantic fingerprint.
    statistics_read_fingerprint: Mutex<Option<StatisticsReadCache>>,
    logical_exprs: Vec<LogicalExprId>,
    /// Exact operator buckets for scoped pattern matching. The bucket is a
    /// read accelerator only: every expression remains in `logical_exprs`,
    /// and merges/rollback rebuild both views together.
    logical_operator_index: BTreeMap<u64, Vec<LogicalExprId>>,
    /// Last Memo-global revision that changed the logical expression set.
    /// Transformation consumers use it to distinguish a completed match from
    /// one whose child frontier has since changed, including through rollback.
    logical_expression_version: u64,
    /// Monotone cursor for the physical implementation domain owned by this
    /// group. Adding an implementation is a dependency of every observed
    /// physical goal; publishing a winner is not. Keeping this separate from
    /// the goal frontiers prevents an unrelated goal publication from
    /// invalidating a parent which reads only one exact child goal.
    physical_implementation_version: u64,
    physical_exprs: Vec<PhysicalExprId>,
    logical_index: BTreeMap<LogicalExprKey, Vec<LogicalExprId>>,
    physical_index: BTreeMap<PhysicalExprKey, PhysicalExprId>,
    winner_frontiers: BTreeMap<OptimizationGoal, WinnerFrontier>,
    /// Each goal owns its own visible candidate revision. A parent read is
    /// keyed by the complete goal (including grant and context), so a change
    /// to goal B cannot invalidate a read of goal A merely because both live
    /// in the same Memo group.
    physical_frontier_versions: BTreeMap<OptimizationGoal, u64>,
    winner_proposals: u64,
    pub ledger: SearchLedger,
}

/// The result of a typed update to the relational facts owned by one group.
///
/// Structural insertion, physical frontier publication, and fact updates are
/// deliberately different Memo events.  Callers which only reconcile facts
/// must not obtain a broad `Group` mutable borrow: that used to invalidate
/// every cached fact fingerprint even when the update was a no-op and made it
/// impossible for the engine to describe the corresponding notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupFactChange {
    pub(crate) group: GroupId,
    pub(crate) logical_changed: bool,
    pub(crate) statistics_changed: bool,
}

impl Group {
    pub fn logical_exprs(&self) -> &[LogicalExprId] {
        &self.logical_exprs
    }

    pub(crate) fn logical_exprs_of_operator(&self, operator_tag: u64) -> &[LogicalExprId] {
        self.logical_operator_index
            .get(&operator_tag)
            .map_or(&[], Vec::as_slice)
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

    fn invalidate_logical_fact_fingerprint(&mut self) {
        self.logical_fact_fingerprint.take();
    }

    fn invalidate_statistics_fingerprints(&mut self) {
        self.statistics_snapshot_fingerprint.take();
        *self
            .statistics_read_fingerprint
            .get_mut()
            .expect("Memo statistics read cache poisoned") = None;
    }

    fn invalidate_fact_fingerprints(&mut self) {
        self.invalidate_logical_fact_fingerprint();
        self.invalidate_statistics_fingerprints();
    }

    pub fn physical_exprs(&self) -> &[PhysicalExprId] {
        &self.physical_exprs
    }

    pub fn physical_implementation_version(&self) -> u64 {
        self.physical_implementation_version
    }

    pub fn physical_frontier_version(&self, goal: OptimizationGoal) -> u64 {
        self.physical_frontier_versions
            .get(&goal)
            .copied()
            .unwrap_or_default()
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
    control: Arc<super::control::SearchControl>,
    groups: Vec<Group>,
    parents: Vec<GroupId>,
    logical_exprs: Vec<LogicalExpr>,
    physical_exprs: Vec<PhysicalExpr>,
    logical_owners: Vec<GroupId>,
    physical_owners: Vec<GroupId>,
    winner_candidates: Vec<WinnerCandidate>,
    winner_proposals: u64,
    group_merges: u64,
    cost_epoch: CostEpoch,
    properties: PropertyInterner,
    optimization_contexts: Vec<OptimizationContext>,
    optimization_context_index: BTreeMap<OptimizationContext, OptimizationContextId>,
    optimization_contexts_frozen: bool,
    logical_frontier_revision: u64,
    budget: Arc<SearchBudget>,
    calibration: Arc<MachineCalibrationBundle>,
    calibration_identity: OnceLock<Fingerprint>,
    regions: Arc<RegionForest>,
    global_ledger: SearchLedger,
    optional_group_budget_sealed: bool,
    cte_producers: BTreeMap<usize, BTreeSet<CteProducerDomain>>,
    cte_producer_insertions: Vec<(usize, CteProducerDomain)>,
    /// Monotone even across rollback/reinsert. Group merges also advance this
    /// cursor because producer correspondences encode canonical GroupIds.
    cte_registry_revision: u64,
    cte_column_types: BTreeMap<(usize, CteColumnId), paro_common::types::LogicalType>,
    cte_type_insertions: Vec<(usize, CteColumnId)>,
    changed_cte_domains: BTreeSet<usize>,
    failed_search_obligations: BTreeSet<super::budget::SearchObligation>,
    /// Search obligations are absorbing for a query-local Memo. Once the
    /// first omission witness is observed, completion checks can stay O(1)
    /// without weakening the detailed ordered report.
    search_obligations_seen: Cell<bool>,
    /// Existing-group fact mutations made by the active transformation
    /// transaction.  Appended groups are handled by the savepoint lengths;
    /// these snapshots keep a rejected staging attempt from leaking a merged
    /// contract/statistics update into the live Memo.
    transformation_group_snapshots:
        Option<BTreeMap<GroupId, (LogicalProperties, GroupCardinality)>>,
}

#[derive(Debug, Clone)]
struct WinnerCandidate {
    group: GroupId,
    goal: OptimizationGoal,
    winner: Arc<Winner>,
}

#[derive(Debug)]
pub(crate) struct TransformationSavepoint {
    group_count: usize,
    logical_expression_count: usize,
    regions: Arc<RegionForest>,
    global_ledger: super::budget::LedgerCheckpoint,
    cte_producer_insertions: usize,
    cte_type_insertions: usize,
    changed_cte_domains: BTreeSet<usize>,
}

impl Memo {
    pub fn new(budget: SearchBudget) -> Self {
        let root_context = OptimizationContext::default();
        let budget = Arc::new(budget);
        let global_ledger = SearchLedger::new(budget.clone());
        Self {
            control: Arc::new(super::control::SearchControl::new(
                budget.optional_time_limit,
            )),
            groups: Vec::new(),
            parents: Vec::new(),
            logical_exprs: Vec::new(),
            physical_exprs: Vec::new(),
            logical_owners: Vec::new(),
            physical_owners: Vec::new(),
            winner_candidates: Vec::new(),
            winner_proposals: 0,
            group_merges: 0,
            cost_epoch: CostEpoch::default(),
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
            calibration_identity: OnceLock::new(),
            regions: Arc::default(),
            global_ledger,
            optional_group_budget_sealed: false,
            cte_producers: BTreeMap::new(),
            cte_producer_insertions: Vec::new(),
            cte_registry_revision: 0,
            cte_column_types: BTreeMap::new(),
            cte_type_insertions: Vec::new(),
            changed_cte_domains: BTreeSet::new(),
            failed_search_obligations: BTreeSet::new(),
            search_obligations_seen: Cell::new(false),
            transformation_group_snapshots: None,
        }
    }

    pub fn set_calibration(&mut self, calibration: Arc<MachineCalibrationBundle>) {
        self.calibration = calibration;
        self.calibration_identity = OnceLock::new();
    }

    pub fn control(&self) -> &Arc<super::control::SearchControl> {
        &self.control
    }

    pub fn set_cancellation(
        &mut self,
        cancellation: paro_context::StatementCancellation,
    ) -> Result<()> {
        Arc::get_mut(&mut self.control)
            .ok_or_else(|| {
                paro_error::internal("search cancellation must be set before sharing control")
            })?
            .set_cancellation(cancellation);
        Ok(())
    }

    /// Exercise wholesale invalidation in the frozen-artifact oracle. The
    /// production engine refreshes exact cost contexts incrementally; opening
    /// a search phase must never use this operation.
    #[cfg(test)]
    pub(crate) fn clear_cost_frontiers(&mut self) -> Result<()> {
        self.cost_epoch = CostEpoch(
            self.cost_epoch
                .0
                .checked_add(1)
                .ok_or_else(|| paro_error::internal("physical cost epoch exhausted"))?,
        );
        for group in &mut self.groups {
            group.winner_frontiers.clear();
            group.physical_frontier_versions.clear();
            group.physical_implementation_version = group
                .physical_implementation_version
                .checked_add(1)
                .ok_or_else(|| paro_error::internal("Memo physical implementation revision overflow"))?;
        }
        Ok(())
    }

    pub(crate) fn cost_epoch(&self) -> CostEpoch {
        self.cost_epoch
    }

    pub(crate) fn cost_epoch_value(&self) -> u64 {
        self.cost_epoch.0
    }

    pub fn calibration(&self) -> &MachineCalibrationBundle {
        self.calibration.as_ref()
    }

    /// The calibration revision is a label, not the complete cost dependency.
    /// The owned bundle is immutable; replacement invalidates this identity.
    pub(crate) fn calibration_fingerprint(&self) -> Fingerprint {
        *self
            .calibration_identity
            .get_or_init(|| self.calibration.stable_fingerprint())
    }

    pub(crate) fn register_cte_producer(
        &mut self,
        cte_index: usize,
        group: GroupId,
        columns: BTreeMap<CteColumnId, super::ids::ColumnId>,
    ) -> Result<()> {
        let group = self.canonical_group(group);
        let schema = &self
            .group(group)
            .ok_or_else(|| paro_error::internal("CTE producer group is missing"))?
            .schema;
        let mut new_types = Vec::new();
        for (definition, column) in &columns {
            let declared = schema
                .columns()
                .iter()
                .find(|entry| entry.id == *column)
                .ok_or_else(|| {
                    paro_error::internal("CTE mapping names a column absent from its producer")
                })?;
            let key = (cte_index, *definition);
            if let Some(previous) = self.cte_column_types.get(&key) {
                if previous != &declared.logical_type {
                    return Err(paro_error::internal(
                        "CTE definition column changes type across producers",
                    ));
                }
            } else {
                new_types.push((key, declared.logical_type.clone()));
            }
        }
        // Publication is atomic: validate every correspondence before changing
        // the registry. Savepoints retain journal cursors, never registry copies.
        for (key, logical_type) in new_types {
            self.cte_column_types.insert(key, logical_type);
            self.cte_type_insertions.push(key);
        }
        let producer = CteProducerDomain { group, columns };
        if self
            .cte_producers
            .entry(cte_index)
            .or_default()
            .insert(producer.clone())
        {
            self.advance_cte_registry_revision()?;
            self.cte_producer_insertions.push((cte_index, producer));
            self.changed_cte_domains.insert(cte_index);
        }
        Ok(())
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
        let revision = if group.logical_properties.cte_references.is_empty() {
            0
        } else {
            self.cte_registry_revision
        };
        let mut cached = group
            .statistics_read_fingerprint
            .lock()
            .expect("Memo statistics read cache poisoned");
        if let Some(previous) = cached.as_ref() {
            if previous.registry_revision == revision
                && previous.producers.iter().copied().eq(self.statistics_read_producers(group))
            {
                return previous.fingerprint;
            }
        }
        let fingerprint = self.compute_local_statistics_fingerprint(group);
        *cached = Some(StatisticsReadCache {
            registry_revision: revision,
            producers: self.statistics_read_producers(group).collect(),
            fingerprint,
        });
        fingerprint
    }

    fn statistics_read_producers<'a>(
        &'a self,
        group: &'a Group,
    ) -> impl Iterator<Item = Option<(Fingerprint, Fingerprint)>> + 'a {
        group.logical_properties.cte_references.iter()
            .flat_map(move |reference| self.cte_producers.get(&reference.cte_index).into_iter().flatten())
            .map(move |producer| self.group(self.canonical_group(producer.group)).map(|group| (
                group.logical_fact_fingerprint(), group.statistics_snapshot_fingerprint(),
            )))
    }

    fn advance_cte_registry_revision(&mut self) -> Result<()> {
        self.cte_registry_revision = self
            .cte_registry_revision
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("Memo CTE registry revision overflow"))?;
        Ok(())
    }

    fn compute_local_statistics_fingerprint(&self, group: &Group) -> Fingerprint {
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
                // Producer group ordinals are Memo-local.  The producer's
                // semantic facts/statistics are already explicit cost
                // dependencies and remain stable when an independent Memo
                // allocates the same CTE forest in a different order.
                if let Some(producer_group) = self.group(self.canonical_group(producer.group)) {
                    fingerprint.write_fingerprint(producer_group.logical_fact_fingerprint());
                    fingerprint.write_fingerprint(producer_group.statistics_snapshot_fingerprint());
                } else {
                    fingerprint.write_bytes(b"missing-cte-producer");
                }
                fingerprint.write_u64(producer.columns.len() as u64);
                for (definition, column) in &producer.columns {
                    fingerprint.write_u64(definition.0 as u64);
                    fingerprint.write_u64(column.0 as u64);
                }
            }
        }
        fingerprint.finish()
    }

    /// Resolve a column domain through the relational group, including a CTE
    /// scan's definition-column dependency on every equivalent producer.
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
                let definition = reference
                    .columns
                    .iter()
                    .find_map(|(definition, candidate)| {
                        (*candidate == column).then_some(definition)
                    })?;
                self.cte_producers
                    .get(&reference.cte_index)?
                    .iter()
                    .filter_map(|producer| {
                        let producer_group = self.group(self.canonical_group(producer.group))?;
                        let producer_column = *producer.columns.get(definition)?;
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
        if group
            .logical_properties
            .conflicting_column_values
            .contains(&column)
        {
            return Ok(None);
        }
        let Some(declared) = group
            .schema
            .columns()
            .iter()
            .find(|entry| entry.id == column)
        else {
            return Ok(None);
        };
        let mut values = None;
        for reference in &group.logical_properties.cte_references {
            let Some(definition) = reference
                .columns
                .iter()
                .find_map(|(definition, candidate)| (*candidate == column).then_some(definition))
            else {
                continue;
            };
            for producer in self
                .cte_producers
                .get(&reference.cte_index)
                .into_iter()
                .flatten()
            {
                let Some(value) = producer.columns.get(definition).and_then(|column| {
                    self.group(producer.group)?
                        .logical_properties
                        .column_values
                        .get(column)
                }) else {
                    continue;
                };
                // Statistics are advisory. Invalid/stale evidence cannot
                // fail compilation or authorize a value-domain rewrite. The
                // explicit column mapping itself is checked at publication.
                if value.statistics().get_type() != &declared.logical_type {
                    return Ok(None);
                }
                values = Some(match values {
                    None => value.clone(),
                    Some(previous) => value.hull(&previous)?,
                });
            }
        }
        Ok(values.or_else(|| {
            group
                .logical_properties
                .column_values
                .get(&column)
                .filter(|value| value.statistics().get_type() == &declared.logical_type)
                .cloned()
        }))
    }

    pub fn set_regions(&mut self, mut regions: RegionForest) {
        regions.recanonicalize_groups(|group| self.canonical_group(group));
        self.regions = Arc::new(regions);
    }

    /// Capture the append-only relational state available to a transformation.
    /// Physical expressions, winners, and property sets are not writable in
    /// this search phase and therefore are intentionally absent.
    pub(crate) fn transformation_savepoint(&mut self) -> TransformationSavepoint {
        debug_assert!(
            self.transformation_group_snapshots.is_none(),
            "nested Memo transformation savepoints are not supported"
        );
        self.transformation_group_snapshots = Some(BTreeMap::new());
        TransformationSavepoint {
            group_count: self.groups.len(),
            logical_expression_count: self.logical_exprs.len(),
            regions: self.regions.clone(),
            global_ledger: self.global_ledger.checkpoint(),
            cte_producer_insertions: self.cte_producer_insertions.len(),
            cte_type_insertions: self.cte_type_insertions.len(),
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
            || savepoint.cte_producer_insertions > self.cte_producer_insertions.len()
            || savepoint.cte_type_insertions > self.cte_type_insertions.len()
        {
            return Err(paro_error::internal(
                "transformation rollback exceeds the current Memo generation",
            ));
        }
        if let Some(snapshots) = self.transformation_group_snapshots.take() {
            for (group, (logical_properties, cardinality)) in snapshots {
                let Some(group) = self.groups.get_mut(group.index()) else {
                    return Err(paro_error::internal(
                        "transformation rollback lost an existing Memo group",
                    ));
                };
                group.logical_properties = logical_properties;
                group.cardinality = cardinality;
                group.invalidate_fact_fingerprints();
            }
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
                let operator_tag = expression
                    .operator_tag
                    .unwrap_or(UNTYPED_LOGICAL_OPERATOR_TAG);
                let remove_operator_bucket = {
                    let bucket = group
                        .logical_operator_index
                        .get_mut(&operator_tag)
                        .ok_or_else(|| {
                            paro_error::internal("transformation rollback lost its operator bucket")
                        })?;
                    let position = bucket
                        .iter()
                        .position(|candidate| *candidate == id)
                        .ok_or_else(|| {
                            paro_error::internal(
                                "transformation rollback lost its operator identity",
                            )
                        })?;
                    bucket.remove(position);
                    bucket.is_empty()
                };
                if remove_operator_bucket {
                    group.logical_operator_index.remove(&operator_tag);
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
            .rollback_to_preserving_exhaustion(savepoint.global_ledger)?;
        if self.cte_producer_insertions.len() != savepoint.cte_producer_insertions {
            self.advance_cte_registry_revision()?;
        }
        for (domain, producer) in self
            .cte_producer_insertions
            .drain(savepoint.cte_producer_insertions..)
        {
            if let Some(producers) = self.cte_producers.get_mut(&domain) {
                producers.remove(&producer);
                if producers.is_empty() {
                    self.cte_producers.remove(&domain);
                }
            }
        }
        for key in self
            .cte_type_insertions
            .drain(savepoint.cte_type_insertions..)
        {
            self.cte_column_types.remove(&key);
        }
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

    /// Finish the active transformation write journal and return the exact
    /// existing groups whose logical facts were touched.  This is consumed by
    /// task publication; a transformation must not exempt every bound input
    /// merely because it happened to read that input.
    pub(crate) fn take_transformation_written_groups(&mut self) -> BTreeSet<GroupId> {
        self.transformation_group_snapshots
            .take()
            .map(|snapshots| snapshots.into_keys().collect())
            .unwrap_or_default()
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
            .declarations()
            .cloned()
            .map(|mut facet| {
                facet.scope = facet
                    .scope
                    .iter()
                    .map(|group| self.canonical_group(*group))
                    .collect();
                facet
            })
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
                    // Priority is a scheduling hint, not facet identity. The
                    // fingerprint intentionally excludes it, so equivalent
                    // expressions that rediscover the same capability merge
                    // at the strongest priority.
                    changed |= existing.merge_declaration(facet)?;
                }
                None => {
                    facets.insert(facet.fingerprint, facet);
                    changed = true;
                }
            }
        }
        if !changed {
            return Ok(self.regions.dropped_optional_facets().collect());
        }
        let regions = RegionForest::normalize(
            facets.into_values(),
            usize::from(self.budget.max_composite_region_groups),
            self.budget.max_mandatory_region_groups as usize,
        )?;
        let dropped = regions.dropped_optional_facets().collect();
        self.regions = Arc::new(regions);
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
            statistics_read_fingerprint: Mutex::new(None),
            logical_exprs: Vec::new(),
            logical_operator_index: BTreeMap::new(),
            logical_expression_version: 0,
            physical_implementation_version: 0,
            physical_exprs: Vec::new(),
            logical_index: BTreeMap::new(),
            physical_index: BTreeMap::new(),
            winner_frontiers: BTreeMap::new(),
            physical_frontier_versions: BTreeMap::new(),
            winner_proposals: 0,
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

    fn record_transformation_group_write(&mut self, id: GroupId) {
        if self.transformation_group_snapshots.is_none() {
            return;
        }
        let id = self.canonical_group(id);
        let already_recorded = self
            .transformation_group_snapshots
            .as_ref()
            .is_some_and(|snapshots| snapshots.contains_key(&id));
        if already_recorded {
            return;
        }
        let Some(group) = self.groups.get(id.index()) else {
            return;
        };
        let snapshot = (group.logical_properties.clone(), group.cardinality.clone());
        self.transformation_group_snapshots
            .as_mut()
            .expect("transformation snapshot disappeared")
            .insert(id, snapshot);
    }

    /// Update only the two fact domains owned by a group.
    ///
    /// The transformation journal is entered before the closure runs, so a
    /// later rollback restores the exact pre-update values.  A failed
    /// closure restores them immediately as well.  Fingerprints and local
    /// statistics caches are invalidated only when their corresponding value
    /// actually changed; publication code can use the returned categories to
    /// wake only the relevant subscribers.
    pub(crate) fn update_group_facts<F>(
        &mut self,
        id: GroupId,
        update: F,
    ) -> Result<GroupFactChange>
    where
        F: FnOnce(&mut LogicalProperties, &mut GroupCardinality) -> Result<()>,
    {
        let id = self.canonical_group(id);
        self.record_transformation_group_write(id);
        let (old_properties, old_cardinality) = {
            let group = self
                .groups
                .get(id.index())
                .ok_or_else(|| paro_error::internal("fact update references an unknown group"))?;
            (group.logical_properties.clone(), group.cardinality.clone())
        };
        let group = self
            .groups
            .get_mut(id.index())
            .ok_or_else(|| paro_error::internal("fact update lost its group"))?;
        if let Err(error) = update(&mut group.logical_properties, &mut group.cardinality) {
            group.logical_properties = old_properties;
            group.cardinality = old_cardinality;
            return Err(error);
        }
        let logical_changed = group.logical_properties != old_properties;
        let statistics_changed = group.cardinality != old_cardinality
            || group.logical_properties.cte_references != old_properties.cte_references;
        if logical_changed {
            group.invalidate_logical_fact_fingerprint();
        }
        if statistics_changed
            || group.logical_properties.cte_references != old_properties.cte_references
        {
            group.invalidate_statistics_fingerprints();
        }
        Ok(GroupFactChange {
            group: id,
            logical_changed,
            statistics_changed,
        })
    }

    /// Publish a derived equivalent relation, not a new statistics observation.
    /// Repeated derivations of the same fact set do not vote on its estimate.
    /// Explicit statistics refreshes use `update_group_facts`; true group
    /// merges retain their deterministic uncertainty merge.
    pub(crate) fn merge_derived_group_facts(
        &mut self,
        id: GroupId,
        incoming: &LogicalProperties,
        cardinality: GroupCardinality,
    ) -> Result<GroupFactChange> {
        let id = self.canonical_group(id);
        let group = self.group(id).ok_or_else(|| {
            paro_error::internal("derived fact publication references an unknown group")
        })?;
        if group.logical_properties == *incoming
            && (group.cardinality.range.is_some() || !group.cardinality.inputs.is_empty())
        {
            // This is not a write: avoid both the rollback snapshot and the
            // merge's temporary copy on the common identical-facts path.
            return Ok(GroupFactChange {
                group: id,
                logical_changed: false,
                statistics_changed: false,
            });
        }
        self.update_group_facts(id, |existing, estimate| {
            let before = existing.clone();
            existing.merge_equivalent_facts(incoming)?;
            if estimate.range.is_none() && estimate.inputs.is_empty() {
                *estimate = cardinality;
            } else if *existing != before {
                // A complete stronger snapshot replaces the older estimate;
                // incomparable fact sets retain uncertainty, never hash-rank
                // competing estimates or elect by rule insertion order.
                *estimate = if existing == incoming
                    && (cardinality.range.is_some() || !cardinality.inputs.is_empty())
                {
                    cardinality
                } else {
                    std::mem::take(estimate).canonical_with(cardinality)
                };
            }
            Ok(())
        })
    }

    /// Escape hatch retained for test fixtures and the Memo verifier.  Hot
    /// production paths must use [`Self::update_group_facts`] or the explicit
    /// structural/frontier APIs above.
    #[cfg(test)]
    pub(crate) fn group_mut(&mut self, id: GroupId) -> Option<&mut Group> {
        let id = self.canonical_group(id);
        self.record_transformation_group_write(id);
        let group = self.groups.get_mut(id.index())?;
        group.invalidate_fact_fingerprints();
        Some(group)
    }

    /// Search accounting is not logical evidence. Mutating a ledger must not
    /// invalidate facts or make subscribed transformations re-read them.
    pub(crate) fn group_ledger_mut(&mut self, id: GroupId) -> Option<&mut SearchLedger> {
        let id = self.canonical_group(id);
        self.groups
            .get_mut(id.index())
            .map(|group| &mut group.ledger)
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

    /// Number of query-local optimization contexts currently interned. The
    /// value is diagnostic evidence for demand-driven context growth; callers
    /// must not use it as a substitute for a context identity or cache.
    pub fn optimization_context_count(&self) -> usize {
        self.optimization_contexts.len()
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
        if self.control.deadline_reached() {
            obligations.insert(SearchObligation {
                group: None,
                reason: SearchIncompleteReason::Deadline,
                witness: Fingerprint(0),
            });
        }
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

    /// Allocation-free completion predicate for the recursive search hot
    /// path. Keep the ordered obligation materialization above as the single
    /// source of detailed audit evidence, but do not rebuild that evidence
    /// while each physical child task is being joined.
    pub(crate) fn search_obligations_empty(&self) -> bool {
        if self.control.deadline_reached() || self.search_obligations_seen.get() {
            return false;
        }
        let empty = self.failed_search_obligations.is_empty()
            && self
                .groups
                .iter()
                .all(|group| self.group_search_obligations_empty(group.id));
        if !empty {
            self.search_obligations_seen.set(true);
        }
        empty
    }

    /// Check completion for one semantic group without materializing the
    /// query-wide obligation set. A physical child may have exhausted its
    /// own logical/physical domain while an unrelated group still has an
    /// optional rule budget outstanding. Requiring the latter to finish
    /// before publishing the child's proof prevents safe parent pruning and
    /// turns every local proof into a query-global barrier.
    ///
    /// Global ledger events remain conservative: they can affect any group,
    /// so no group-local certificate is emitted while one is present. The
    /// query-wide `search_obligations_empty` predicate remains the only
    /// criterion for declaring the root search complete.
    pub(crate) fn group_search_obligations_empty(&self, group: GroupId) -> bool {
        if self.control.deadline_reached() || self.global_ledger.has_exhaustion_events() {
            return false;
        }
        let group = self.canonical_group(group);
        if self.failed_search_obligations.iter().any(|obligation| {
            obligation
                .group
                .is_some_and(|owner| self.canonical_group(owner) == group)
        }) {
            return false;
        }
        self.group(group)
            .is_some_and(|group| !group.ledger.has_exhaustion_events())
    }

    /// Check the local physical search contract without importing unrelated
    /// query-global group reservations.  A physical task has already
    /// enumerated its current recipes when its own group ledger is clean; a
    /// global group/composition reservation exhausted in another branch does
    /// not make that exact, already-published physical domain incomplete.
    ///
    /// This is intentionally weaker than [`Self::group_search_obligations_empty`]
    /// and must only be used for a versioned local physical certificate.  Any
    /// later logical, fact, statistics, or child-frontier publication changes
    /// the task ReadSet and invalidates the certificate.  The root's global
    /// completion predicate continues to use the stronger method above.
    pub(crate) fn group_physical_obligations_empty(&self, group: GroupId) -> bool {
        if self.control.deadline_reached() {
            return false;
        }
        let group = self.canonical_group(group);
        if self.failed_search_obligations.iter().any(|obligation| {
            obligation
                .group
                .is_some_and(|owner| self.canonical_group(owner) == group)
        }) {
            return false;
        }
        self.group(group)
            .is_some_and(|group| !group.ledger.has_exhaustion_events())
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

    pub(crate) fn record_deferred_grant(&mut self, class: super::ids::ResourceGrantClassId) {
        self.failed_search_obligations
            .insert(super::budget::SearchObligation {
                group: None,
                reason: super::budget::SearchIncompleteReason::OptionalGrantDeferred(class),
                witness: Fingerprint(u128::from(class.0)),
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
        Ok(self.intern_context_value(context))
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
        self.intern_demand_context(
            base,
            context.filterable_sources,
            context.phase,
            context.ownership,
            context.continuation,
        )
    }

    /// Intern a context demanded by an already-bound physical task. Unlike
    /// the initial expression-path catalog, this method is allowed after the
    /// catalog is frozen: only an actual child source/response demand can
    /// create the variant, so optional search cannot pre-expand a context
    /// powerset.
    pub(super) fn intern_demand_context(
        &mut self,
        base: OptimizationContextId,
        sources: BTreeSet<super::rules::WorkSourceId>,
        phase: OptimizationPhase,
        ownership: SharedOwnership,
        continuation: ContinuationContract,
    ) -> Result<OptimizationContextId> {
        let mut context = self
            .optimization_context(base)
            .cloned()
            .ok_or_else(|| paro_error::internal("demand has no optimization context"))?;
        context.filterable_sources = sources;
        context.phase = phase;
        context.ownership = ownership;
        context.continuation = continuation;
        Ok(self.intern_context_value(context))
    }

    fn intern_context_value(&mut self, context: OptimizationContext) -> OptimizationContextId {
        if let Some(id) = self.optimization_context_index.get(&context) {
            return *id;
        }
        let id = OptimizationContextId::new(self.optimization_contexts.len());
        self.optimization_contexts.push(context.clone());
        self.optimization_context_index.insert(context, id);
        id
    }

    pub fn insert_logical(
        &mut self,
        target: GroupId,
        key: LogicalExprKey,
        payload: LogicalPayloadId,
        proof: EquivalenceProof,
    ) -> Result<LogicalExprId> {
        self.insert_logical_structural(target, key, payload, proof, None, None)
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
            Some(Arc::from(operator_encoding.clone())),
            operator_tag_from_recorded_encoding(&operator_encoding),
        )
    }

    pub(crate) fn insert_logical_with_operator_encoding_and_tag(
        &mut self,
        target: GroupId,
        key: LogicalExprKey,
        payload: LogicalPayloadId,
        proof: EquivalenceProof,
        operator_encoding: Box<[u8]>,
        operator_tag: u64,
    ) -> Result<LogicalExprId> {
        self.insert_logical_structural(
            target,
            key,
            payload,
            proof,
            Some(Arc::from(operator_encoding)),
            Some(operator_tag),
        )
    }

    /// Lower one complete planner node and its fact snapshot atomically.
    ///
    /// The caller has already validated any rule-specific output contract,
    /// but it must not insert the structural expression and then merge facts
    /// after committing the surrounding transformation.  Keeping both parts
    /// in this operation gives rollback/retry one journal boundary and makes
    /// the publication path consume the exact key, encoding and facts that
    /// staging produced.
    pub(crate) fn insert_logical_with_facts(
        &mut self,
        contract: LogicalInsertionContract,
    ) -> Result<LogicalExprId> {
        let LogicalInsertionContract {
            target,
            key,
            payload,
            operator_encoding,
            proof,
            logical_properties,
            cardinality,
        } = contract;
        let logical = match operator_encoding {
            Some(encoding) => self.insert_logical_with_operator_encoding(
                target,
                key,
                payload,
                proof,
                encoding,
            )?,
            None => self.insert_logical(target, key, payload, proof)?,
        };
        self.merge_derived_group_facts(target, &logical_properties, cardinality)?;
        Ok(logical)
    }

    fn insert_logical_structural(
        &mut self,
        target: GroupId,
        mut key: LogicalExprKey,
        payload: LogicalPayloadId,
        proof: EquivalenceProof,
        operator_encoding: Option<Arc<[u8]>>,
        operator_tag: Option<u64>,
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
        self.record_transformation_group_write(target);
        let id = LogicalExprId::new(self.logical_exprs.len());
        self.logical_exprs.push(LogicalExpr {
            id,
            key: key.clone(),
            operator_encoding,
            operator_tag,
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
        group
            .logical_operator_index
            .entry(operator_tag.unwrap_or(UNTYPED_LOGICAL_OPERATOR_TAG))
            .or_default()
            .push(id);
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
        group.physical_implementation_version = group
            .physical_implementation_version
            .checked_add(1)
            .ok_or_else(|| {
                paro_error::internal("Memo physical implementation revision overflow")
            })?;
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
        let context = self
            .optimization_contexts
            .get(goal.context.index())
            .ok_or_else(|| paro_error::internal("winner has no source-demand context"))?;
        // Assign a permanent identity only when the proposal is published.
        // A rejected proposal cannot yet have a parent reference; once an ID
        // escapes, its single immutable allocation remains in the archive even
        // after later frontier pruning or a group merge.
        winner.candidate = CandidateId::new(self.winner_candidates.len());
        let physical_fingerprint = winner.physical_fingerprint;
        let frontier_limit = self.budget.max_winner_frontier_candidates_per_goal.max(1) as usize;
        self.winner_proposals = self.winner_proposals.saturating_add(1);
        self.groups[group.index()].winner_proposals = self.groups[group.index()]
            .winner_proposals
            .saturating_add(1);
        let slot = self.groups[group.index()].winner_frontiers.entry(goal);
        let insertion = match slot {
            std::collections::btree_map::Entry::Vacant(entry) => {
                let mut frontier = WinnerFrontier::default();
                // Only a new frontier owns a separate demand set.
                frontier.filterable_sources = context.filterable_sources.clone();
                let insertion = frontier.insert_with_limit(goal, winner, frontier_limit);
                entry.insert(frontier);
                insertion
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => entry
                .get_mut()
                .insert_with_limit(goal, winner, frontier_limit),
        };
        let _partition = crate::work_partition::enter(crate::work_partition::Bucket::Publish);
        let frontier_changed =
            insertion.selected_changed || insertion.truncated || insertion.published.is_some();
        if insertion.truncated {
            self.groups[group.index()].ledger.record_budget_limited(
                BudgetDimension::WinnerFrontier,
                winner_frontier_budget_witness(group, goal, physical_fingerprint),
            );
        }
        if let Some(winner) = insertion.published {
            self.winner_candidates.push(WinnerCandidate {
                group,
                goal,
                winner,
            });
        }
        if frontier_changed {
            let group = self
                .groups
                .get_mut(group.index())
                .ok_or_else(|| paro_error::internal("winner group disappeared"))?;
            let revision = group
                .physical_frontier_versions
                .entry(goal)
                .or_default();
            *revision = revision
                .checked_add(1)
                .ok_or_else(|| paro_error::internal("Memo physical goal revision overflow"))?;
        }
        Ok(insertion.selected_changed)
    }

    /// Test the bounded frontier with only the parent-observable summary.
    /// Callers use this before allocating child/proof payloads for a
    /// candidate that cannot be published.
    pub(crate) fn candidate_preview(
        &self,
        group: GroupId,
        goal: OptimizationGoal,
        candidate: CandidateSummary<'_>,
    ) -> Result<CandidatePreview> {
        let group = self.canonical_group(group);
        let Some(group) = self.group(group) else {
            return Err(paro_error::internal(
                "candidate summary references an unknown group",
            ));
        };
        Ok(group
            .winner_frontier(goal)
            .map_or(CandidatePreview::Publish, |frontier| {
                frontier.preview(
                    goal,
                    candidate,
                    self.budget.max_winner_frontier_candidates_per_goal as usize,
                )
            }))
    }

    /// Preserve proposal accounting when a summary proves that the owned
    /// candidate payload cannot enter the frontier.  No candidate archive or
    /// child tuple is allocated for this path.
    pub(crate) fn record_rejected_winner_proposal(
        &mut self,
        group: GroupId,
        goal: OptimizationGoal,
        physical_fingerprint: Fingerprint,
        truncated: bool,
    ) -> Result<()> {
        let group = self.canonical_group(group);
        let group = self.groups.get_mut(group.index()).ok_or_else(|| {
            paro_error::internal("candidate rejection references an unknown group")
        })?;
        self.winner_proposals = self.winner_proposals.saturating_add(1);
        group.winner_proposals = group.winner_proposals.saturating_add(1);
        if let Some(frontier) = group.winner_frontiers.get_mut(&goal) {
            if truncated {
                frontier.record_truncated_proposal();
            } else {
                frontier.record_rejected_proposal();
            }
        }
        if truncated {
            group.ledger.record_budget_limited(
                BudgetDimension::WinnerFrontier,
                winner_frontier_budget_witness(group.id, goal, physical_fingerprint),
            );
        }
        Ok(())
    }

    pub fn winner_proposal_count(&self) -> u64 {
        self.winner_proposals
    }

    pub fn published_winner_count(&self) -> u64 {
        self.winner_candidates.len() as u64
    }

    pub fn resolve_child_winner(&self, child: ChildWinnerRef) -> Option<&Winner> {
        let candidate = self.winner_candidates.get(child.candidate.index())?;
        (self.canonical_group(candidate.group) == self.canonical_group(child.group)
            && candidate.goal == child.goal)
            .then_some(&candidate.winner)
    }

    pub fn resolve_child_winner_arc(&self, child: ChildWinnerRef) -> Option<Arc<Winner>> {
        let candidate = self.winner_candidates.get(child.candidate.index())?;
        (self.canonical_group(candidate.group) == self.canonical_group(child.group)
            && candidate.goal == child.goal)
            .then(|| candidate.winner.clone())
    }

    /// Freeze exactly one candidate DAG for handoff to extraction/execution.
    ///
    /// This deliberately walks only the selected winner and its exact child
    /// references.  It does not copy Memo groups, frontiers, recipes, or
    /// unselected alternatives.  A candidate cycle or an unresolved payload
    /// is rejected at the handoff boundary instead of being repaired by a
    /// later search pass.
    pub fn freeze_candidate_tree(&self, root: ChildWinnerRef) -> Result<Arc<FrozenCandidate>> {
        let _partition = crate::work_partition::enter(crate::work_partition::Bucket::QualityFreeze);
        fn visit(
            memo: &Memo,
            reference: ChildWinnerRef,
            active: &mut BTreeSet<CandidateId>,
            cache: &mut BTreeMap<CandidateId, Arc<FrozenCandidate>>,
        ) -> Result<Arc<FrozenCandidate>> {
            if let Some(frozen) = cache.get(&reference.candidate) {
                if frozen.reference.group == reference.group
                    && frozen.reference.goal == reference.goal
                {
                    return Ok(frozen.clone());
                }
                return Err(paro_error::internal(
                    "candidate identity was reused with a different group or goal",
                ));
            }
            if !active.insert(reference.candidate) {
                return Err(paro_error::internal(
                    "winner candidate DAG contains a cycle",
                ));
            }
            let winner = memo
                .resolve_child_winner_arc(reference)
                .ok_or_else(|| paro_error::internal("candidate references an unknown winner"))?;
            let physical = memo
                .physical_expr(winner.expression)
                .cloned()
                .ok_or_else(|| {
                    paro_error::internal("candidate references an unknown physical expression")
                })?;
            let logical = memo
                .logical_expr(physical.key.logical)
                .cloned()
                .ok_or_else(|| {
                    paro_error::internal("candidate physical payload lost its logical expression")
                })?;
            let mut children = Vec::with_capacity(winner.children.len());
            for child in winner.children.iter().copied() {
                children.push(visit(memo, child, active, cache)?);
            }
            active.remove(&reference.candidate);
            let frozen = Arc::new(FrozenCandidate {
                reference,
                winner,
                logical: Arc::new(logical),
                physical: Arc::new(physical),
                children: children.into_boxed_slice(),
            });
            cache.insert(reference.candidate, frozen.clone());
            Ok(frozen)
        }

        visit(self, root, &mut BTreeSet::new(), &mut BTreeMap::new())
    }

    pub fn merge_groups(&mut self, left: GroupId, right: GroupId) -> Result<GroupId> {
        if left.index() >= self.parents.len() || right.index() >= self.parents.len() {
            return Err(paro_error::internal(
                "cannot merge Memo groups with an unknown group id",
            ));
        }
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

        // All operations below the union-find redirect are part of one
        // observable merge. Validate every fallible step against temporary
        // values before mutating `parents` or either group; otherwise a
        // statistics value-hull error or revision overflow could leave a
        // redirect whose facts and subscriptions no longer describe the same
        // equivalence class.
        let next_cte_registry_revision = self
            .cte_registry_revision
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("Memo CTE registry revision overflow"))?;
        let next_logical_frontier_revision = self
            .logical_frontier_revision
            .checked_add(1)
            .ok_or_else(|| paro_error::internal("Memo frontier revision overflow"))?;
        let mut merged_logical_properties =
            self.groups[canonical.index()].logical_properties.clone();
        merged_logical_properties
            .merge_equivalent_facts(&self.groups[secondary.index()].logical_properties)?;
        let merged_cardinality = self.groups[canonical.index()]
            .cardinality
            .clone()
            .canonical_with(self.groups[secondary.index()].cardinality.clone());

        self.cte_registry_revision = next_cte_registry_revision;
        self.parents[secondary.index()] = canonical;
        self.group_merges = self.group_merges.saturating_add(1);
        self.logical_frontier_revision = next_logical_frontier_revision;

        let (canonical_group, secondary_group) =
            two_groups_mut(&mut self.groups, canonical.index(), secondary.index());
        canonical_group.invalidate_fact_fingerprints();
        secondary_group.invalidate_fact_fingerprints();
        canonical_group.winner_proposals = canonical_group
            .winner_proposals
            .saturating_add(secondary_group.winner_proposals);
        secondary_group.winner_proposals = 0;
        // Equivalent expressions can establish different conservative row
        // bounds (for example, a decorrelated plan can prove a tighter cap
        // than its dependent form). Both proofs describe the same relation,
        // so their intersection is valid for the complete equivalence class.
        canonical_group.logical_properties = merged_logical_properties;
        canonical_group.cardinality = merged_cardinality;
        secondary_group.cardinality = GroupCardinality::default();
        canonical_group.ledger.merge_from(&secondary_group.ledger);
        canonical_group
            .logical_exprs
            .append(&mut secondary_group.logical_exprs);
        for (operator_tag, expressions) in
            std::mem::take(&mut secondary_group.logical_operator_index)
        {
            canonical_group
                .logical_operator_index
                .entry(operator_tag)
                .or_default()
                .extend(expressions);
        }
        canonical_group.logical_expression_version = self.logical_frontier_revision;
        canonical_group
            .physical_exprs
            .append(&mut secondary_group.physical_exprs);
        canonical_group.physical_implementation_version = canonical_group
            .physical_implementation_version
            .max(secondary_group.physical_implementation_version)
            .checked_add(1)
            .ok_or_else(|| {
                paro_error::internal("Memo physical implementation revision overflow")
            })?;
        canonical_group.winner_frontiers.clear();
        secondary_group.winner_frontiers.clear();
        canonical_group.physical_frontier_versions.clear();
        secondary_group.physical_frontier_versions.clear();

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
        let mut regions = (*self.regions).clone();
        regions.recanonicalize_groups(&canonical);
        self.regions = Arc::new(regions);
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
            group.logical_operator_index.clear();
            group.physical_index.clear();
            group.winner_frontiers.clear();
            group.physical_frontier_versions.clear();
            group.physical_implementation_version = group
                .physical_implementation_version
                .saturating_add(1);
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
                group
                    .logical_operator_index
                    .entry(
                        self.logical_exprs[expression.index()]
                            .operator_tag
                            .unwrap_or(UNTYPED_LOGICAL_OPERATOR_TAG),
                    )
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

/// Planner operator identities use the recording form of
/// `StableFingerprintBuilder`: the first field after the domain prefix is the
/// tagged operator kind. Keep parsing an optional accelerator here so the
/// generic Memo API remains source-compatible with core-only callers. A
/// malformed or foreign encoding simply goes into the untyped bucket and is
/// still visited by the complete matcher.
fn operator_tag_from_recorded_encoding(encoding: &[u8]) -> Option<u64> {
    const PREFIX: &[u8] = b"paro.stable-fingerprint.v3.blake3";
    let offset = PREFIX.len();
    (encoding.len() >= offset.saturating_add(1 + std::mem::size_of::<u64>())
        && &encoding[..offset] == PREFIX
        && encoding[offset] == 2)
        .then(|| {
            let start = offset + 1;
            let end = start + std::mem::size_of::<u64>();
            u64::from_le_bytes(
                encoding[start..end]
                    .try_into()
                    .expect("checked operator tag"),
            )
        })
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

#[cfg(test)]
#[path = "memo/region_delta_tests.rs"]
mod region_delta_tests;
