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
    pub cost_composition: CostComposition,
    pub cost: SearchCost,
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
    // 0=risk/latency, 1=throughput, 2=memory, 3=robustness. Unknown extension
    // profiles use the conservative risk ordering until their registry owns
    // comparison during extraction.
    match objective.0 {
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
        _ => left
            .cost
            .score
            .risk_adjusted
            .total_cmp(&right.cost.score.risk_adjusted)
            .then_with(|| {
                left.cost
                    .critical_path
                    .expected
                    .total_cmp(&right.cost.critical_path.expected)
            })
            .then_with(|| {
                left.cost
                    .peak_memory_upper
                    .cmp(&right.cost.peak_memory_upper)
            }),
    }
}

#[derive(Debug)]
pub struct Group {
    pub id: GroupId,
    pub schema: GroupSchema,
    pub logical_properties: LogicalProperties,
    logical_exprs: Vec<LogicalExprId>,
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
        Self {
            groups: Vec::new(),
            parents: Vec::new(),
            logical_exprs: Vec::new(),
            physical_exprs: Vec::new(),
            logical_owners: Vec::new(),
            physical_owners: Vec::new(),
            properties: PropertyInterner::default(),
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
                let group = &mut self.groups[owner.index()];
                if group.logical_exprs.pop() != Some(id)
                    || group.logical_index.remove(&expression.key) != Some(id)
                {
                    return Err(paro_error::internal(
                        "transformation rollback found inconsistent group membership",
                    ));
                }
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
                    || existing.priority != facet.priority
                {
                    return Err(paro_error::internal(
                        "planning facet fingerprint changed its contract",
                    ));
                }
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
    ) -> GroupId {
        let id = GroupId::new(self.groups.len());
        self.groups.push(Group {
            id,
            schema,
            logical_properties,
            logical_exprs: Vec::new(),
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
        let group = &mut self.groups[target.index()];
        group.logical_index.insert(key, id);
        group.logical_exprs.push(id);
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
        let recomputed_cost = recompute_winner_cost(self, &winner)?;
        if recomputed_cost != winner.cost {
            return Err(paro_error::internal(
                "winner cumulative cost disagrees with local/child/enforcer composition",
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

        let (canonical_group, secondary_group) =
            two_groups_mut(&mut self.groups, canonical.index(), secondary.index());
        // Equivalent expressions can establish different conservative row
        // bounds (for example, a decorrelated plan can prove a tighter cap
        // than its dependent form). Both proofs describe the same relation,
        // so their intersection is valid for the complete equivalence class.
        canonical_group.logical_properties.maximum_cardinality = match (
            canonical_group.logical_properties.maximum_cardinality,
            secondary_group.logical_properties.maximum_cardinality,
        ) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(bound), None) | (None, Some(bound)) => Some(bound),
            (None, None) => None,
        };
        canonical_group.ledger.merge_from(&secondary_group.ledger);
        canonical_group
            .logical_exprs
            .append(&mut secondary_group.logical_exprs);
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

fn recompute_winner_cost(memo: &Memo, winner: &Winner) -> Result<SearchCost> {
    let mut child_costs = Vec::with_capacity(winner.child_goals.len());
    for (child, child_goal) in winner.child_goals.iter().copied() {
        let child_cost = memo
            .group(child)
            .and_then(|group| group.winner(child_goal))
            .ok_or_else(|| paro_error::internal("winner cost replay lost a child winner"))?
            .cost;
        child_costs.push(child_cost);
    }
    let cost = super::engine::compose_candidate_cost(
        winner.local_cost,
        &child_costs,
        winner.cost_composition,
    )?;
    let enforcer_cost = super::engine::enforcer_cost(
        &winner.enforcers,
        winner.enforcer_cost_input,
        memo.calibration(),
    )?
    .ok_or_else(|| paro_error::internal("recorded winner has an infeasible enforcer grant"))?;
    cost.sequential(enforcer_cost)
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
