// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Finite property enforcement: one canonical baseline plus registered recipes.

use std::collections::{BTreeMap, BTreeSet};

use paro_common::error::{self as paro_error, Result};

use super::ids::{EnforcerRecipeId, Fingerprint, MutationBarrierId, StableFingerprintBuilder};
use super::properties::{
    MutationSafetyRequirement, OrderingRequirement, OrderingScope, PartitioningRequirement,
    ProvidedMutationSafety, ProvidedOrdering, ProvidedPartitioning, ProvidedProperties,
    ProvidedReplayability, ProvidedRepresentation, ReplayabilityRequirement,
    RepresentationRequirement, RequiredOrdering, RequiredProperties,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EnforcerStep {
    Gather,
    RepartitionHash {
        keys: Box<[super::ids::ColumnId]>,
        partitions: u16,
    },
    RepartitionRange {
        keys: Box<[super::ids::ColumnId]>,
        partitions: u16,
    },
    Sort(RequiredOrdering),
    LocalSort(RequiredOrdering),
    MergeGather(RequiredOrdering),
    PrepareOrderedFetch(RequiredOrdering),
    Fetch {
        values: BTreeSet<super::ids::ColumnId>,
    },
    FetchPreservingOrder {
        values: BTreeSet<super::ids::ColumnId>,
        ordering: RequiredOrdering,
    },
    MutationInputSpool {
        barrier: MutationBarrierId,
    },
    Flatten,
    Factorize(super::ids::FactorizationSpecId),
    Spool,
}

/// Physical conversions implemented by the current execution ABI.
///
/// Search owns this capability boundary: an enforcer that cannot be lowered
/// must never become a costed candidate or winner and fail later in extraction.
#[derive(Debug, Clone, Copy)]
struct ExecutableEnforcers {
    all: bool,
}

impl ExecutableEnforcers {
    const LOCAL_RUNTIME: Self = Self { all: false };

    #[cfg(test)]
    const ALL: Self = Self { all: true };

    fn supports(self, step: &EnforcerStep) -> bool {
        if self.all {
            return true;
        }
        match step {
            EnforcerStep::Sort(ordering) => {
                ordering.scope == OrderingScope::Global
                    && ordering.keys.iter().all(|key| key.collation.is_none())
            }
            EnforcerStep::MutationInputSpool { .. } => true,
            EnforcerStep::Gather
            | EnforcerStep::RepartitionHash { .. }
            | EnforcerStep::RepartitionRange { .. }
            | EnforcerStep::LocalSort(_)
            | EnforcerStep::MergeGather(_)
            | EnforcerStep::PrepareOrderedFetch(_)
            | EnforcerStep::Fetch { .. }
            | EnforcerStep::FetchPreservingOrder { .. }
            | EnforcerStep::Flatten
            | EnforcerStep::Factorize(_)
            | EnforcerStep::Spool => false,
        }
    }
}

impl EnforcerStep {
    pub(crate) fn apply(
        &self,
        mut provided: ProvidedProperties,
        required: &RequiredProperties,
    ) -> Result<ProvidedProperties> {
        match self {
            Self::Gather => {
                provided.partitioning = ProvidedPartitioning::Singleton;
                provided.ordering = ProvidedOrdering::Unordered;
            }
            Self::RepartitionHash { keys, partitions } => {
                provided.partitioning = ProvidedPartitioning::Hash {
                    keys: keys.clone(),
                    partitions: *partitions,
                };
                provided.ordering = ProvidedOrdering::Unordered;
            }
            Self::RepartitionRange { keys, partitions } => {
                provided.partitioning = ProvidedPartitioning::Range {
                    keys: keys.clone(),
                    partitions: *partitions,
                };
                provided.ordering = ProvidedOrdering::Unordered;
            }
            Self::Sort(ordering) => {
                if ordering.scope == OrderingScope::Global {
                    provided.partitioning = ProvidedPartitioning::Singleton;
                }
                provided.ordering = ProvidedOrdering::Ordered {
                    keys: ordering.keys.clone(),
                    scope: ordering.scope,
                };
            }
            Self::LocalSort(ordering) => {
                provided.ordering = ProvidedOrdering::Ordered {
                    keys: ordering.keys.clone(),
                    scope: OrderingScope::PartitionLocal,
                };
            }
            Self::MergeGather(ordering) => {
                let ProvidedOrdering::Ordered {
                    keys,
                    scope: OrderingScope::PartitionLocal,
                } = &provided.ordering
                else {
                    return Err(paro_error::internal(
                        "MergeGather requires partition-local ordering",
                    ));
                };
                if keys.as_ref() != ordering.keys.as_ref() {
                    return Err(paro_error::internal(
                        "MergeGather local/global ordering keys disagree",
                    ));
                }
                provided.partitioning = ProvidedPartitioning::Singleton;
                provided.ordering = ProvidedOrdering::Ordered {
                    keys: ordering.keys.clone(),
                    scope: OrderingScope::Global,
                };
            }
            Self::PrepareOrderedFetch(_) => {
                // A legal zero-property-progress preparation step. Its effect
                // is private to the registered compound implementation.
            }
            Self::Fetch { values } => {
                if provided.materialization.locators.is_empty() {
                    return Err(paro_error::internal("Fetch requires a stable locator"));
                }
                provided
                    .materialization
                    .values
                    .extend(values.iter().copied());
            }
            Self::FetchPreservingOrder { values, ordering } => {
                if provided.materialization.locators.is_empty() {
                    return Err(paro_error::internal(
                        "ordered Fetch requires a stable locator",
                    ));
                }
                provided
                    .materialization
                    .values
                    .extend(values.iter().copied());
                provided.ordering = ProvidedOrdering::Ordered {
                    keys: ordering.keys.clone(),
                    scope: ordering.scope,
                };
                if ordering.scope == OrderingScope::Global {
                    provided.partitioning = ProvidedPartitioning::Singleton;
                }
            }
            Self::MutationInputSpool { barrier } => {
                let MutationSafetyRequirement::StableReadBeforeWrite { targets, snapshot } =
                    &required.mutation_safety
                else {
                    return Err(paro_error::internal(
                        "MutationInputSpool used without a mutation-safety requirement",
                    ));
                };
                provided.mutation_safety = ProvidedMutationSafety::MaterializedMutationInput {
                    targets: targets.clone(),
                    snapshot: *snapshot,
                    barrier: *barrier,
                };
                provided.replayability = ProvidedReplayability::Rewindable;
            }
            Self::Flatten => provided.representation = ProvidedRepresentation::Flat,
            Self::Factorize(spec) => {
                provided.representation = ProvidedRepresentation::Factorized(*spec);
                provided.ordering = ProvidedOrdering::Unordered;
            }
            Self::Spool => provided.replayability = ProvidedReplayability::Rewindable,
        }
        provided.validate()?;
        Ok(provided)
    }

    pub(crate) fn stable_fingerprint(&self) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        match self {
            Self::Gather => builder.write_u64(0),
            Self::RepartitionHash { keys, partitions } => {
                builder.write_u64(1);
                encode_columns(&mut builder, keys);
                builder.write_u64(u64::from(*partitions));
            }
            Self::RepartitionRange { keys, partitions } => {
                builder.write_u64(2);
                encode_columns(&mut builder, keys);
                builder.write_u64(u64::from(*partitions));
            }
            Self::Sort(ordering) => {
                builder.write_u64(3);
                encode_ordering(&mut builder, ordering);
            }
            Self::LocalSort(ordering) => {
                builder.write_u64(4);
                encode_ordering(&mut builder, ordering);
            }
            Self::MergeGather(ordering) => {
                builder.write_u64(5);
                encode_ordering(&mut builder, ordering);
            }
            Self::PrepareOrderedFetch(ordering) => {
                builder.write_u64(6);
                encode_ordering(&mut builder, ordering);
            }
            Self::Fetch { values } => {
                builder.write_u64(7);
                encode_columns(&mut builder, values);
            }
            Self::FetchPreservingOrder { values, ordering } => {
                builder.write_u64(8);
                encode_columns(&mut builder, values);
                encode_ordering(&mut builder, ordering);
            }
            Self::MutationInputSpool { barrier } => {
                builder.write_u64(9);
                builder.write_u64(barrier.0 as u64);
            }
            Self::Flatten => builder.write_u64(10),
            Self::Factorize(spec) => {
                builder.write_u64(11);
                builder.write_u64(spec.0 as u64);
            }
            Self::Spool => builder.write_u64(12),
        }
        builder.finish()
    }
}

fn encode_columns<'a>(
    builder: &mut StableFingerprintBuilder,
    columns: impl IntoIterator<Item = &'a super::ids::ColumnId>,
) {
    let columns = columns.into_iter().collect::<Vec<_>>();
    builder.write_u64(columns.len() as u64);
    for column in columns {
        builder.write_u64(column.0 as u64);
    }
}

fn encode_ordering(builder: &mut StableFingerprintBuilder, ordering: &RequiredOrdering) {
    builder.write_u64(match ordering.scope {
        OrderingScope::PartitionLocal => 0,
        OrderingScope::Global => 1,
    });
    builder.write_u64(ordering.keys.len() as u64);
    for key in &ordering.keys {
        builder.write_u64(key.column.0 as u64);
        builder.write_u64(match key.direction {
            super::properties::SortDirection::Asc => 0,
            super::properties::SortDirection::Desc => 1,
        });
        builder.write_u64(match key.nulls {
            super::properties::NullOrder::First => 0,
            super::properties::NullOrder::Last => 1,
        });
        match key.collation {
            Some(collation) => {
                builder.write_u64(1);
                builder.write_u64(collation.0 as u64);
            }
            None => builder.write_u64(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnforcedPlan {
    pub steps: Box<[EnforcerStep]>,
    pub provided: ProvidedProperties,
}

#[derive(Debug, Clone)]
pub struct EnforcerRecipeTemplate {
    pub id: EnforcerRecipeId,
    pub max_steps: u8,
}

#[derive(Debug, Clone)]
pub struct EnforcerRecipe {
    pub template: EnforcerRecipeId,
    pub steps: Box<[EnforcerStep]>,
}

impl EnforcerRecipe {
    fn fingerprint(&self) -> Fingerprint {
        let mut builder = StableFingerprintBuilder::default();
        builder.write_u64(self.template.0 as u64);
        builder.write_u64(self.steps.len() as u64);
        for step in &self.steps {
            builder.write_fingerprint(step.stable_fingerprint());
        }
        builder.finish()
    }
}

#[derive(Debug)]
pub struct EnforcementPlanner {
    templates: BTreeMap<EnforcerRecipeId, EnforcerRecipeTemplate>,
    max_optional_depth: u8,
    max_optional_chains: u8,
    seen_recipes: BTreeSet<Fingerprint>,
    executable: ExecutableEnforcers,
}

impl EnforcementPlanner {
    pub fn new(max_optional_depth: u8, max_optional_chains: u8) -> Self {
        Self {
            templates: BTreeMap::new(),
            max_optional_depth,
            max_optional_chains,
            seen_recipes: BTreeSet::new(),
            executable: ExecutableEnforcers::LOCAL_RUNTIME,
        }
    }

    #[cfg(test)]
    fn with_all_for_test(max_optional_depth: u8, max_optional_chains: u8) -> Self {
        Self {
            executable: ExecutableEnforcers::ALL,
            ..Self::new(max_optional_depth, max_optional_chains)
        }
    }

    fn executable(&self, steps: &[EnforcerStep]) -> bool {
        steps.iter().all(|step| self.executable.supports(step))
    }

    pub fn register(&mut self, template: EnforcerRecipeTemplate) -> Result<()> {
        if template.max_steps == 0 || template.max_steps > self.max_optional_depth {
            return Err(paro_error::internal(
                "enforcer recipe template has an invalid static length",
            ));
        }
        if self.templates.insert(template.id, template).is_some() {
            return Err(paro_error::internal("duplicate enforcer recipe id"));
        }
        Ok(())
    }

    pub fn canonical_baseline(
        &self,
        provided: ProvidedProperties,
        required: &RequiredProperties,
    ) -> Result<Option<EnforcedPlan>> {
        required.validate()?;
        if !provided
            .result_guarantee
            .satisfies(required.result_guarantee)
        {
            return Ok(None);
        }
        let mut state = provided;
        let mut steps = Vec::new();

        if !state.mutation_safety.satisfies(&required.mutation_safety) {
            let step = EnforcerStep::MutationInputSpool {
                barrier: MutationBarrierId(0),
            };
            state = step.apply(state, required)?;
            steps.push(step);
        }
        if !required
            .materialization
            .values
            .is_subset(&state.materialization.values)
        {
            let step = EnforcerStep::Fetch {
                values: required.materialization.values.clone(),
            };
            state = step.apply(state, required)?;
            steps.push(step);
        }
        if !state.representation.satisfies(&required.representation) {
            let step = match required.representation {
                RepresentationRequirement::Any => unreachable!(),
                RepresentationRequirement::Flat => EnforcerStep::Flatten,
                RepresentationRequirement::Factorized(spec) => EnforcerStep::Factorize(spec),
            };
            state = step.apply(state, required)?;
            steps.push(step);
        }
        if !state.replayability.satisfies(required.replayability) {
            if required.replayability == ReplayabilityRequirement::Rewindable {
                let step = EnforcerStep::Spool;
                state = step.apply(state, required)?;
                steps.push(step);
            }
        }
        if !state.partitioning.satisfies(&required.partitioning) {
            let step = match &required.partitioning {
                PartitioningRequirement::Any => unreachable!(),
                PartitioningRequirement::Singleton => EnforcerStep::Gather,
                PartitioningRequirement::Hash { keys, partitions } => {
                    EnforcerStep::RepartitionHash {
                        keys: keys.clone(),
                        partitions: partitions.unwrap_or(1),
                    }
                }
                PartitioningRequirement::Range { keys, partitions } => {
                    EnforcerStep::RepartitionRange {
                        keys: keys.clone(),
                        partitions: partitions.unwrap_or(1),
                    }
                }
            };
            state = step.apply(state, required)?;
            steps.push(step);
        }
        if !state.ordering.satisfies(&required.ordering) {
            if let OrderingRequirement::Ordered(ordering) = &required.ordering {
                let step = EnforcerStep::Sort(ordering.clone());
                state = step.apply(state, required)?;
                steps.push(step);
            }
        }
        if !state.satisfies(required) {
            return Err(paro_error::internal(
                "canonical enforcer baseline failed to satisfy the goal",
            ));
        }
        if !self.executable(&steps) {
            return Ok(None);
        }
        Ok(Some(EnforcedPlan {
            steps: steps.into_boxed_slice(),
            provided: state,
        }))
    }

    pub fn instantiate_optional(
        &mut self,
        provided: ProvidedProperties,
        required: &RequiredProperties,
        recipe: EnforcerRecipe,
    ) -> Result<Option<EnforcedPlan>> {
        let template = self
            .templates
            .get(&recipe.template)
            .ok_or_else(|| paro_error::internal("unregistered enforcer recipe"))?;
        if recipe.steps.is_empty()
            || recipe.steps.len() > template.max_steps as usize
            || recipe.steps.len() > self.max_optional_depth as usize
        {
            return Err(paro_error::internal(
                "enforcer recipe exceeds its registered static bound",
            ));
        }
        if !self.executable(&recipe.steps) {
            return Ok(None);
        }
        let fingerprint = recipe.fingerprint();
        if self.seen_recipes.contains(&fingerprint) {
            return Ok(None);
        }
        if self.seen_recipes.len() >= self.max_optional_chains as usize {
            return Ok(None);
        }

        let mut state = provided;
        for step in &recipe.steps {
            state = step.apply(state, required)?;
        }
        if !state.satisfies(required) {
            return Err(paro_error::internal(
                "registered enforcer recipe does not satisfy its final goal",
            ));
        }
        self.seen_recipes.insert(fingerprint);
        Ok(Some(EnforcedPlan {
            steps: recipe.steps,
            provided: state,
        }))
    }
}

pub(crate) fn replay_enforcer_chain(
    mut provided: ProvidedProperties,
    required: &RequiredProperties,
    steps: &[EnforcerStep],
) -> Result<ProvidedProperties> {
    for step in steps {
        provided = step.apply(provided, required)?;
    }
    Ok(provided)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::ids::{BaseRelationId, ColumnId, LocatorKindId, SnapshotId};
    use crate::cascades::properties::{
        LocatorDescriptor, MaterializationRequirement, MutationSafetyRequirement, NullOrder,
        OrderingKey, PartitioningRequirement, ProvidedMaterialization, ResultGuarantee,
        SortDirection,
    };

    fn ordering(scope: OrderingScope) -> RequiredOrdering {
        RequiredOrdering {
            keys: vec![OrderingKey {
                column: ColumnId(1),
                direction: SortDirection::Asc,
                nulls: NullOrder::Last,
                collation: None,
            }]
            .into_boxed_slice(),
            scope,
        }
    }

    fn provided() -> ProvidedProperties {
        ProvidedProperties {
            ordering: ProvidedOrdering::Unordered,
            partitioning: ProvidedPartitioning::Hash {
                keys: vec![ColumnId(7)].into_boxed_slice(),
                partitions: 4,
            },
            materialization: ProvidedMaterialization {
                values: BTreeSet::new(),
                locators: [(
                    BaseRelationId(1),
                    LocatorDescriptor {
                        kind: LocatorKindId(1),
                        snapshot: SnapshotId(1),
                        write_target: false,
                    },
                )]
                .into_iter()
                .collect(),
            },
            mutation_safety: ProvidedMutationSafety::NotApplicable,
            representation: ProvidedRepresentation::Flat,
            replayability: ProvidedReplayability::OnePass,
            result_guarantee: ResultGuarantee::Exact,
        }
    }

    fn required_global_order() -> RequiredProperties {
        RequiredProperties {
            ordering: OrderingRequirement::Ordered(ordering(OrderingScope::Global)),
            partitioning: PartitioningRequirement::Any,
            materialization: MaterializationRequirement {
                values: [ColumnId(1)].into_iter().collect(),
                locators: BTreeMap::new(),
            },
            mutation_safety: MutationSafetyRequirement::None,
            representation: RepresentationRequirement::Flat,
            replayability: ReplayabilityRequirement::Any,
            result_guarantee: ResultGuarantee::Exact,
        }
    }

    #[test]
    fn canonical_baseline_is_finite_and_satisfies_goal() {
        let planner = EnforcementPlanner::with_all_for_test(8, 8);
        let plan = planner
            .canonical_baseline(provided(), &required_global_order())
            .unwrap()
            .unwrap();
        assert!(plan.provided.satisfies(&required_global_order()));
        assert_eq!(plan.steps.len(), 2);
    }

    #[test]
    fn registered_recipe_may_have_zero_property_progress_first_step() {
        let mut planner = EnforcementPlanner::with_all_for_test(4, 4);
        planner
            .register(EnforcerRecipeTemplate {
                id: EnforcerRecipeId(1),
                max_steps: 2,
            })
            .unwrap();
        let required = required_global_order();
        let result = planner
            .instantiate_optional(
                provided(),
                &required,
                EnforcerRecipe {
                    template: EnforcerRecipeId(1),
                    steps: vec![
                        EnforcerStep::PrepareOrderedFetch(ordering(OrderingScope::Global)),
                        EnforcerStep::FetchPreservingOrder {
                            values: [ColumnId(1)].into_iter().collect(),
                            ordering: ordering(OrderingScope::Global),
                        },
                    ]
                    .into_boxed_slice(),
                },
            )
            .unwrap()
            .expect("recipe should be admitted");
        assert!(result.provided.satisfies(&required));
    }

    #[test]
    fn unregistered_or_overlong_recipe_is_rejected() {
        let mut planner = EnforcementPlanner::new(2, 2);
        let recipe = EnforcerRecipe {
            template: EnforcerRecipeId(9),
            steps: vec![EnforcerStep::Spool].into_boxed_slice(),
        };
        assert!(planner
            .instantiate_optional(provided(), &required_global_order(), recipe)
            .is_err());
    }
}
