//! Contextual Memo with expression-local rule history and goal-keyed winners.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::budget::{SearchBudget, SearchLedger};
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
use super::region::{JointCostProof, RegionForest};
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

    pub fn regions(&self) -> &RegionForest {
        &self.regions
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
        if self.groups[canonical.index()].schema != self.groups[secondary.index()].schema
            || self.groups[canonical.index()].logical_properties
                != self.groups[secondary.index()].logical_properties
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
mod tests {
    use paro_common::types::LogicalType;

    use super::*;
    use crate::cascades::column::{ColumnDesc, ColumnOrigin, ColumnVisibility};
    use crate::cascades::cost::{CompactRange, ScoreSummary};
    use crate::cascades::properties::{
        MaterializationRequirement, MutationSafetyRequirement, OrderingRequirement,
        PartitioningRequirement, ProvidedMaterialization, ProvidedMutationSafety, ProvidedOrdering,
        ProvidedPartitioning, ProvidedReplayability, ProvidedRepresentation,
        ReplayabilityRequirement, RepresentationRequirement, ResultGuarantee,
    };

    fn schema(column: u32) -> GroupSchema {
        GroupSchema::new([ColumnDesc {
            id: super::super::ids::ColumnId(column),
            logical_type: LogicalType::Integer,
            nullable: false,
            origin: ColumnOrigin::Derived {
                key: Fingerprint(column as u128),
            },
            visibility: ColumnVisibility::Visible,
            name_hint: None,
        }])
        .unwrap()
    }

    fn provided() -> ProvidedProperties {
        ProvidedProperties {
            ordering: ProvidedOrdering::Unordered,
            partitioning: ProvidedPartitioning::Singleton,
            materialization: ProvidedMaterialization::default(),
            mutation_safety: ProvidedMutationSafety::NotApplicable,
            representation: ProvidedRepresentation::Flat,
            replayability: ProvidedReplayability::OnePass,
            result_guarantee: ResultGuarantee::Exact,
        }
    }

    fn enforcer_cost_input() -> super::super::engine::EnforcerCostInput {
        super::super::engine::EnforcerCostInput::unbounded(CompactRange::point(1.0).unwrap(), 8)
    }

    fn required() -> RequiredProperties {
        RequiredProperties {
            ordering: OrderingRequirement::Any,
            partitioning: PartitioningRequirement::Any,
            materialization: MaterializationRequirement::default(),
            mutation_safety: MutationSafetyRequirement::None,
            representation: RepresentationRequirement::Any,
            replayability: ReplayabilityRequirement::Any,
            result_guarantee: ResultGuarantee::Exact,
        }
    }

    #[test]
    fn rule_history_is_expression_local_and_duplicate_expr_is_deduped() {
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(schema(1), LogicalProperties::default());
        let key = LogicalExprKey {
            operator: Fingerprint(10),
            scalars: Box::new([]),
            children: Box::new([]),
        };
        let expression = memo
            .insert_logical(
                group,
                key.clone(),
                LogicalPayloadId(0),
                EquivalenceProof::Initial,
            )
            .unwrap();
        let duplicate = memo
            .insert_logical(
                group,
                key,
                LogicalPayloadId(0),
                EquivalenceProof::Normalization { rule: RuleId(1) },
            )
            .unwrap();
        assert_eq!(expression, duplicate);
        assert!(memo.mark_rule_applied(expression, RuleId(7)).unwrap());
        assert!(!memo.mark_rule_applied(expression, RuleId(7)).unwrap());
    }

    #[test]
    fn winner_is_keyed_by_goal_and_uses_stable_tie_break() {
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(schema(1), LogicalProperties::default());
        let logical = memo
            .insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(1),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId(0),
                EquivalenceProof::Initial,
            )
            .unwrap();
        let properties = provided();
        let physical = memo
            .insert_physical(
                group,
                PhysicalExprKey {
                    implementation: ImplementationId(1),
                    logical,
                    children: Box::new([]),
                    payload_fingerprint: Fingerprint(1),
                },
                PhysicalPayloadId(0),
                properties.clone(),
            )
            .unwrap();
        let required = memo.intern_required(required()).unwrap();
        let goal = OptimizationGoal {
            required,
            row_goal: RowGoal::All,
            objective: ObjectiveProfileId(0),
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        let cost = SearchCost {
            score: ScoreSummary {
                range: CompactRange::point(1.0).unwrap(),
                risk_adjusted: 1.0,
            },
            ..SearchCost::ZERO
        };
        memo.record_winner(
            group,
            goal,
            Winner {
                expression: physical,
                child_goals: Box::new([]),
                enforcers: Box::new([]),
                enforcer_cost_input: enforcer_cost_input(),
                provided: properties.clone(),
                local_cost: cost,
                cost_composition: CostComposition::Sequential,
                cost,
                physical_fingerprint: Fingerprint(20),
                joint_cost_proof: None,
            },
        )
        .unwrap();
        memo.record_winner(
            group,
            goal,
            Winner {
                expression: physical,
                child_goals: Box::new([]),
                enforcers: Box::new([]),
                enforcer_cost_input: enforcer_cost_input(),
                provided: properties,
                local_cost: cost,
                cost_composition: CostComposition::Sequential,
                cost,
                physical_fingerprint: Fingerprint(10),
                joint_cost_proof: None,
            },
        )
        .unwrap();
        assert_eq!(
            memo.group(group)
                .unwrap()
                .winner(goal)
                .unwrap()
                .physical_fingerprint,
            Fingerprint(10)
        );
    }

    #[test]
    fn exact_tie_keeps_the_mandatory_expression_ahead_of_ephemeral_fingerprints() {
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(schema(1), LogicalProperties::default());
        let baseline_logical = memo
            .insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(100),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId(0),
                EquivalenceProof::Initial,
            )
            .unwrap();
        let optional_logical = memo
            .insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(200),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId(1),
                EquivalenceProof::Transformation {
                    rule: RuleId(7),
                    source: baseline_logical,
                    premise: Fingerprint(100),
                },
            )
            .unwrap();
        let properties = provided();
        let baseline = memo
            .insert_physical(
                group,
                PhysicalExprKey {
                    implementation: ImplementationId(1),
                    logical: baseline_logical,
                    children: Box::new([]),
                    payload_fingerprint: Fingerprint(u128::MAX),
                },
                PhysicalPayloadId(0),
                properties.clone(),
            )
            .unwrap();
        let optional = memo
            .insert_physical(
                group,
                PhysicalExprKey {
                    implementation: ImplementationId(1),
                    logical: optional_logical,
                    children: Box::new([]),
                    payload_fingerprint: Fingerprint(0),
                },
                PhysicalPayloadId(1),
                properties.clone(),
            )
            .unwrap();
        let goal = OptimizationGoal {
            required: memo.intern_required(required()).unwrap(),
            row_goal: RowGoal::All,
            objective: ObjectiveProfileId(0),
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        let cost = SearchCost {
            score: ScoreSummary {
                range: CompactRange::point(1.0).unwrap(),
                risk_adjusted: 1.0,
            },
            ..SearchCost::ZERO
        };
        for (expression, fingerprint) in [
            (optional, Fingerprint(0)),
            (baseline, Fingerprint(u128::MAX)),
        ] {
            memo.record_winner(
                group,
                goal,
                Winner {
                    expression,
                    child_goals: Box::new([]),
                    enforcers: Box::new([]),
                    enforcer_cost_input: enforcer_cost_input(),
                    provided: properties.clone(),
                    local_cost: cost,
                    cost_composition: CostComposition::Sequential,
                    cost,
                    physical_fingerprint: fingerprint,
                    joint_cost_proof: None,
                },
            )
            .unwrap();
        }
        assert_eq!(
            memo.group(group).unwrap().winner(goal).unwrap().expression,
            baseline
        );
    }

    #[test]
    fn winner_frontier_retains_non_dominated_resource_tradeoffs() {
        let mut budget = SearchBudget::default();
        budget.max_pareto_winners_per_goal = 4;
        let mut memo = Memo::new(budget);
        let group = memo.create_group(schema(1), LogicalProperties::default());
        let logical = memo
            .insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(1),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId(0),
                EquivalenceProof::Initial,
            )
            .unwrap();
        let properties = provided();
        let physical = memo
            .insert_physical(
                group,
                PhysicalExprKey {
                    implementation: ImplementationId(1),
                    logical,
                    children: Box::new([]),
                    payload_fingerprint: Fingerprint(1),
                },
                PhysicalPayloadId(0),
                properties.clone(),
            )
            .unwrap();
        let goal = OptimizationGoal {
            required: memo.intern_required(required()).unwrap(),
            row_goal: RowGoal::All,
            objective: ObjectiveProfileId(0),
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        for (score, memory, fingerprint) in [(1.0, 100, 1), (2.0, 10, 2), (3.0, 200, 3)] {
            let cost = SearchCost {
                score: ScoreSummary {
                    range: CompactRange::point(score).unwrap(),
                    risk_adjusted: score,
                },
                critical_path: CompactRange::point(score).unwrap(),
                peak_memory_upper: memory,
                ..SearchCost::ZERO
            };
            memo.record_winner(
                group,
                goal,
                Winner {
                    expression: physical,
                    child_goals: Box::new([]),
                    enforcers: Box::new([]),
                    enforcer_cost_input: enforcer_cost_input(),
                    provided: properties.clone(),
                    local_cost: cost,
                    cost_composition: CostComposition::Sequential,
                    cost,
                    physical_fingerprint: Fingerprint(fingerprint),
                    joint_cost_proof: None,
                },
            )
            .unwrap();
        }
        let frontier = memo.group(group).unwrap().winner_frontier(goal).unwrap();
        assert_eq!(frontier.candidates().len(), 2);
        assert_eq!(
            frontier.selected().unwrap().physical_fingerprint,
            Fingerprint(1)
        );
    }

    #[test]
    fn group_merge_rejects_output_contract_change() {
        let mut memo = Memo::new(SearchBudget::default());
        let left = memo.create_group(schema(1), LogicalProperties::default());
        let right = memo.create_group(schema(2), LogicalProperties::default());
        assert!(memo.merge_groups(left, right).is_err());
    }

    #[test]
    fn winner_recording_recomputes_local_cost_instead_of_trusting_total() {
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(schema(1), LogicalProperties::default());
        let logical = memo
            .insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(1),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId(0),
                EquivalenceProof::Initial,
            )
            .unwrap();
        let properties = provided();
        let physical = memo
            .insert_physical(
                group,
                PhysicalExprKey {
                    implementation: ImplementationId(1),
                    logical,
                    children: Box::new([]),
                    payload_fingerprint: Fingerprint(1),
                },
                PhysicalPayloadId(0),
                properties.clone(),
            )
            .unwrap();
        let goal = OptimizationGoal {
            required: memo.intern_required(required()).unwrap(),
            row_goal: RowGoal::All,
            objective: ObjectiveProfileId(0),
            grant: GrantGoalKey::Invariant(AdmissibleGrantSetId(0)),
            context: OptimizationContextId(0),
        };
        let local = SearchCost {
            score: ScoreSummary {
                range: CompactRange::point(1.0).unwrap(),
                risk_adjusted: 1.0,
            },
            critical_path: CompactRange::point(1.0).unwrap(),
            ..SearchCost::ZERO
        };
        let falsified = SearchCost {
            score: ScoreSummary {
                range: CompactRange::point(0.5).unwrap(),
                risk_adjusted: 0.5,
            },
            critical_path: CompactRange::point(0.5).unwrap(),
            ..SearchCost::ZERO
        };
        assert!(memo
            .record_winner(
                group,
                goal,
                Winner {
                    expression: physical,
                    child_goals: Box::new([]),
                    enforcers: Box::new([]),
                    enforcer_cost_input: enforcer_cost_input(),
                    provided: properties,
                    local_cost: local,
                    cost_composition: CostComposition::Sequential,
                    cost: falsified,
                    physical_fingerprint: Fingerprint(1),
                    joint_cost_proof: None,
                },
            )
            .is_err());
    }
}
