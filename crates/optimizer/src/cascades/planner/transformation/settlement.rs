// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Node-local settlement over immutable arena edges and value-interned facts.
//!
//! A cache entry owns an operator recipe, not a representative descendant
//! tree. Its key includes ordered input facts and the lexical CTE producer
//! domain actually read. Revisions and unrelated sibling bindings are not
//! semantic inputs. Evaluation occurrences are attached only after lookup.

use super::*;
use paro_common::types::LogicalType;
use paro_planner::operator::{BoundReference, LogicalOutputLayout};
use paro_planner::plan::arena::{LogicalPlanArena, LogicalPlanNode, PlanIndex};
use paro_planner::plan::{NodeStats, PlanNodeId};
use std::hash::{Hash, Hasher};

pub(super) mod demand;

type FactId = usize;
type CteEnvironment = Arc<BTreeMap<usize, FactId>>;

#[derive(Debug)]
struct RelationFacts {
    layout: LogicalOutputLayout,
    stats: NodeStats,
    maximum: Option<u64>,
    columns: Vec<Arc<ColumnStatistics>>,
    column_ids: Box<[usize]>,
    /// Immutable layout columns in this settlement cache's native namespace.
    /// Populate on first use, at the original interning point, so caching does
    /// not rename scalar identities or change bounded-search scheduling.
    binding_columns: Option<Box<[ColumnId]>>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ColumnFactKey {
    logical_type: LogicalType,
    storage: Vec<u8>,
    distinct: paro_storage::statistics::DistinctEvidence,
    distribution: Option<paro_storage::statistics::EstimatedNumericDistribution>,
    storage_observation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LocalKey {
    operator: Box<[u8]>,
    scalars: Box<[ScalarExprId]>,
    output: Box<[ColumnId]>,
    inputs: Box<[FactId]>,
    cte: Option<(usize, FactId)>,
    input_stats: NodeStats,
}

/// Cheap admission key used before constructing the complete local recipe
/// identity.  A shape hit means a full key may be worth materializing; a
/// shape miss proves that no cached local can match and avoids layout
/// interning, scalar serialization, and operator identity encoding on the
/// overwhelmingly common first-visit path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LocalShape {
    operator_tag: Box<[u8]>,
    inputs: Box<[FactId]>,
    input_stats: NodeStats,
}

fn operator_shape_tag<Child>(
    operator: &LogicalOperator<Child>,
    scalars: &ScalarArena,
) -> Result<Box<[u8]>> {
    // Scalar-free shells can be fingerprinted without cloning or interning
    // their children. Reuse the canonical operator encoding so a fast hit is
    // never allowed to alias two filters/orders that differ only in their
    // output projection map.
    if operator_has_no_scalar_payload(operator)
        && !matches!(operator, LogicalOperator::BoundReference(_))
    {
        let encoding = query_operator_identity(operator, &[], scalars)?.1;
        let mut builder = StableFingerprintBuilder::recording();
        builder.write_bytes(b"paro.settlement-local-shape.v2");
        builder.write_bytes(&encoding);
        match operator {
            LogicalOperator::Filter(filter) => {
                encode_projection_map(&mut builder, &filter.projection_map)
            }
            LogicalOperator::Order(order) => {
                encode_projection_map(&mut builder, &order.projection_map)
            }
            LogicalOperator::TopN(topn) => {
                encode_projection_map(&mut builder, &topn.projection_map)
            }
            _ => {}
        }
        return Ok(builder.finish_recording().1);
    }
    let mut builder = StableFingerprintBuilder::recording();
    builder.write_bytes(b"paro.settlement-local-shape.v2");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::mem::discriminant(operator).hash(&mut hasher);
    builder.write_u64(hasher.finish());
    Ok(builder.finish_recording().1)
}

/// Only the shared scalar visitor may establish absence of expression roots.
/// The operator whitelist keeps unsupported payloads out of the fast path.
fn operator_has_no_scalar_payload<Child>(operator: &LogicalOperator<Child>) -> bool {
    if let LogicalOperator::Get(get) = operator {
        return get.runtime_filter_expressions.is_empty();
    }
    if !matches!(
        operator,
        LogicalOperator::Filter(_)
            | LogicalOperator::Projection(_)
            | LogicalOperator::RowFetch(_)
            | LogicalOperator::Limit(_)
            | LogicalOperator::Order(_)
            | LogicalOperator::TopN(_)
            | LogicalOperator::Distinct(_)
            | LogicalOperator::EmptyResult(_)
            | LogicalOperator::MaterializedCTE(_)
            | LogicalOperator::RecursiveCTE(_)
            | LogicalOperator::CTERef(_)
            | LogicalOperator::SetOperation(_)
            | LogicalOperator::DummyScan
    ) {
        return false;
    }
    // Share the scalar enumeration contract with lowering. LIMIT/OFFSET,
    // DISTINCT ON targets and row-fetch rowids are scalar payloads too;
    // their presence must never be guessed from the operator tag.
    let mut has_scalar = false;
    paro_planner::visitor::enumerate_expression_refs(operator, |_| has_scalar = true);
    !has_scalar
}

#[derive(Debug, Clone)]
struct SettledLocal {
    recipe: PlanIndex,
    facts: FactId,
    shape: LocalShape,
    /// Statistics needed by this operator's expressions, including columns
    /// which are consumed locally but not returned by its projection map.
    statistics: SharedColumnStatistics,
}

#[derive(Debug, Default)]
pub(in crate::cascades::planner) struct SettlementCache {
    columns: ColumnCatalog,
    bindings: BindingCatalog,
    scalars: ScalarArena,
    locals: BTreeMap<LocalKey, SettledLocal>,
    recipe_prefix: Option<paro_planner::plan::arena::PlanArenaCheckpoint>,
    local_shapes: BTreeSet<LocalShape>,
    /// Exact scalar-free shape buckets used for an allocation-free cache hit.
    /// The BTreeSet above remains the cheap negative filter for scalar-bearing
    /// operators whose full identity depends on interned expression roots.
    local_shape_entries: BTreeMap<LocalShape, Vec<SettledLocal>>,
    facts: Vec<RelationFacts>,
    facts_by_columns: BTreeMap<Box<[usize]>, Vec<FactId>>,
    /// Interned column facts include evidence provenance, not just the
    /// serialized storage payload.  The latter intentionally omits planner
    /// metadata such as partial coverage, so two byte-identical sketches can
    /// still have different proof contracts (ObservedFull vs
    /// ObservedPartial/Derived) and must never share a ColumnId.
    column_values: HashMap<ColumnFactKey, usize>,
    /// Pointer fast path for live statistics allocations.  A `Weak` keeps
    /// the cache from pinning every column fact for the lifetime of the
    /// planner session while still making address reuse safe: an upgraded
    /// weak pointer can only match the original allocation.
    column_pointers: HashMap<usize, (std::sync::Weak<ColumnStatistics>, usize)>,
    scan_bindings: demand::ScanBindings,
    pub(in crate::cascades::planner) hits: u64,
    pub(in crate::cascades::planner) misses: u64,
    pub(in crate::cascades::planner) invalidation_visits: u64,
    pub(in crate::cascades::planner) input_column_cache_hits: u64,
    pub(in crate::cascades::planner) input_column_cache_misses: u64,
    #[cfg(test)]
    test_arena: LogicalPlanArena,
}

pub(super) struct SettledExpression<Plan = PlanIndex> {
    pub(super) plan: Plan,
    pub(super) statistics: SharedColumnStatistics,
    pub(super) scopes: HashMap<PlanNodeId, SharedColumnStatistics>,
}

fn intern_columns_into(
    bindings: &mut BindingCatalog,
    columns: &mut ColumnCatalog,
    layout: &LogicalOutputLayout,
) -> Result<Box<[ColumnId]>> {
    layout
        .bindings()
        .iter()
        .zip(layout.types())
        .map(|(binding, ty)| {
            if let Some(id) = bindings.get(binding.table_index, binding.column_index, ty) {
                return Ok(*id);
            }
            let column = columns.intern(
                ty.clone(),
                true,
                ColumnOrigin::Internal {
                    key: Fingerprint(columns.len() as u128),
                },
                ColumnVisibility::Visible,
                None,
            )?;
            bindings.insert(binding.table_index, binding.column_index, ty, column)?;
            Ok(column)
        })
        .collect()
}

impl SettlementCache {
    pub(in crate::cascades::planner) fn bound_import_counts(&self) -> (u64, u64) {
        self.scalars.bound_import_counts()
    }
    pub(in crate::cascades::planner) fn discard_stale_recipes(&mut self, arena: &LogicalPlanArena) {
        if self
            .recipe_prefix
            .is_none_or(|prefix| arena.retains_prefix(prefix))
        {
            return;
        }
        self.invalidation_visits = self
            .invalidation_visits
            .saturating_add(self.locals.len() as u64);
        self.locals.retain(|_, entry| arena.owns(entry.recipe));
        // A shape is only a fast negative filter. Rebuild it from the live
        // recipes after rollback so a stale shape can never turn a future
        // first visit into an incorrect cache hit.
        self.local_shapes.clear();
        self.local_shape_entries.clear();
        for entry in self.locals.values() {
            self.local_shapes.insert(entry.shape.clone());
            self.local_shape_entries
                .entry(entry.shape.clone())
                .or_default()
                .push(entry.clone());
        }
        self.recipe_prefix = Some(arena.checkpoint());
    }

    fn intern_fact(&mut self, mut fact: RelationFacts) -> Result<FactId> {
        let mut columns = Vec::with_capacity(fact.columns.len());
        for column in &fact.columns {
            let pointer = Arc::as_ptr(column) as usize;
            let id = if let Some((weak, id)) = self.column_pointers.get(&pointer) {
                if weak
                    .upgrade()
                    .is_some_and(|existing| Arc::ptr_eq(&existing, column))
                {
                    *id
                } else {
                    self.column_pointers.remove(&pointer);
                    self.intern_column_value(column)?
                }
            } else {
                self.intern_column_value(column)?
            };
            columns.push(id);
        }
        fact.column_ids = columns.into_boxed_slice();
        let candidates = self
            .facts_by_columns
            .entry(fact.column_ids.clone())
            .or_default();
        if let Some(id) = candidates.iter().find(|id| {
            let prior = &self.facts[**id];
            prior.layout == fact.layout
                && prior.stats == fact.stats
                && prior.maximum == fact.maximum
        }) {
            return Ok(*id);
        }
        let id = self.facts.len();
        candidates.push(id);
        self.facts.push(fact);
        Ok(id)
    }

    fn intern_column_value(&mut self, column: &Arc<ColumnStatistics>) -> Result<usize> {
        let pointer = Arc::as_ptr(column) as usize;
        let key = ColumnFactKey {
            logical_type: column.statistics().get_type().clone(),
            storage: column.to_bytes()?,
            distinct: column.distinct_evidence(),
            distribution: column.estimated_numeric_distribution(),
            storage_observation: column.is_storage_observation(),
        };
        let next = self.column_values.len();
        let id = *self.column_values.entry(key).or_insert(next);
        self.column_pointers
            .insert(pointer, (Arc::downgrade(column), id));
        Ok(id)
    }

    fn boundary(&self, ordinal: usize, fact: FactId) -> Result<OwnedLogicalPlan> {
        let fact = &self.facts[fact];
        let mut reference = BoundReference::new(
            paro_planner::operator::BoundReferenceId::input_ordinal(ordinal),
            fact.layout.bindings().to_vec(),
            fact.layout.types().to_vec(),
        );
        let mut domain =
            paro_planner::operator::bound_reference::BoundRelationFactValues::default();
        domain.cardinality = fact.stats.estimated_cardinality;
        domain.maximum_cardinality = fact.maximum;
        domain.unique_keys = fact.stats.unique_keys.clone();
        domain.column_domains = fact
            .columns
            .iter()
            .map(
                |column| paro_planner::operator::bound_reference::BoundColumnDomain {
                    expected_distinct: Some(column.distinct_evidence().point)
                        .filter(|value| *value > 0)
                        // An estimated row point is not a semantic upper
                        // bound.  Only `fact.maximum` is proof-backed and
                        // may constrain a distinct domain; preserving the
                        // estimator point prevents low-cardinality columns
                        // from collapsing when a sibling alternative has a
                        // one-row estimate.
                        .map(|distinct| fact.maximum.map_or(distinct, |rows| distinct.min(rows))),
                    guaranteed_distinct_upper: column.guaranteed_distinct_upper(),
                    provenance: column.distinct_evidence().provenance,
                },
            )
            .collect();
        domain.column_values = fact
            .columns
            .iter()
            .map(|column| {
                paro_planner::operator::bound_reference::BoundColumnValues::from_column(column)
                    .map(Some)
            })
            .collect::<Result<Vec<_>>>()?;
        reference.facts = Arc::new(
            paro_planner::operator::bound_reference::BoundRelationFacts::new(
                domain,
                fact.layout.types().to_vec(),
            ),
        );
        Ok(OwnedLogicalPlan {
            id: PlanNodeId::SYNTHETIC,
            stats: fact.stats.clone(),
            operator: LogicalOperator::BoundReference(reference),
        })
    }

    fn local(
        &mut self,
        shell: LogicalPlanNode<()>,
        inputs: &[FactId],
        ctes: &CteEnvironment,
        environment: &PlannerRuleEnvironment,
        recipes: &mut LogicalPlanArena,
    ) -> Result<SettledLocal> {
        let scalar_free = operator_has_no_scalar_payload(&shell.operator);
        let shape = LocalShape {
            operator_tag: operator_shape_tag(&shell.operator, &self.scalars)?,
            inputs: inputs.into(),
            input_stats: shell.stats.clone(),
        };
        // A scalar-free shell has a complete identity before output layouts
        // or column/scalar catalogs are touched. Probe the exact shape bucket
        // first; only a surviving recipe in the caller's arena may be
        // returned. This removes the dominant hit-path copies while keeping a
        // full-key fallback for scalar-bearing or CTE-environment-sensitive
        // operators.
        if scalar_free && !matches!(shell.operator, LogicalOperator::CTERef(_)) {
            let candidate = self
                .local_shape_entries
                .get(&shape)
                .into_iter()
                .flat_map(|entries| entries.iter())
                // The map key already proves the complete cheap shape.  Do
                // not recompute the operator fingerprint for every hit; only
                // the arena ownership and borrowed output-layout contract
                // remain to be checked before reusing the cached local.
                .find(|entry| recipes.owns(entry.recipe))
                .cloned();
            if let Some(entry) = candidate {
                // The shape tag is intentionally cheap and therefore only a
                // necessary condition.  Re-derive the output layout from the
                // current child facts before publishing a hit.  This is a
                // small borrowed-layout operation (and is skipped entirely
                // on the miss path until the full recipe is needed), but it
                // prevents a future operator/layout contract drift from
                // aliasing two scalar-free locals in the same bucket.
                let layout_matches =
                    recipes
                        .output_layout(entry.recipe)
                        .ok()
                        .is_some_and(|cached| {
                            self.output_layout_matches(&shell.operator, inputs, cached)
                        });
                if layout_matches {
                    self.hits += 1;
                    return Ok(entry);
                }
            }
        }
        let maybe_cached = self.local_shapes.contains(&shape);
        let child_layout_refs = inputs
            .iter()
            .map(|id| &self.facts[*id].layout)
            .collect::<Vec<_>>();
        let child_maxima = inputs
            .iter()
            .map(|id| self.facts[*id].maximum)
            .collect::<Vec<_>>();
        let output = shell
            .operator
            .output_layout_from_child_refs(&child_layout_refs);
        drop(child_layout_refs);
        let output_columns = intern_columns_into(&mut self.bindings, &mut self.columns, &output)?;
        for id in inputs {
            let fact = self.facts.get_mut(*id).ok_or_else(|| {
                paro_error::internal("settlement input references an unknown fact")
            })?;
            if fact.binding_columns.is_none() {
                fact.binding_columns = Some(intern_columns_into(
                    &mut self.bindings,
                    &mut self.columns,
                    &fact.layout,
                )?);
                self.input_column_cache_misses += 1;
            } else {
                self.input_column_cache_hits += 1;
            }
        }
        let child_columns = inputs
            .iter()
            .map(|id| {
                self.facts[*id].binding_columns.as_deref().ok_or_else(|| {
                    paro_error::internal("settlement input has no interned layout columns")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (roots, operator_identity) = if scalar_free {
            let roots: Box<[ScalarExprId]> = Box::new([]);
            let identity = query_operator_identity(&shell.operator, &roots, &self.scalars)?.1;
            (roots, identity)
        } else {
            let roots = intern_operator_scalars(
                &shell.operator,
                &output_columns,
                &child_columns,
                &mut self.bindings,
                &mut self.columns,
                &mut self.scalars,
            )?;
            let identity = query_operator_identity(&shell.operator, &roots, &self.scalars)?.1;
            (roots, identity)
        };
        let cte = if let LogicalOperator::CTERef(reference) = &shell.operator {
            ctes.get(&reference.cte_index)
                .map(|fact| (reference.cte_index, *fact))
        } else {
            None
        };
        let key = LocalKey {
            operator: operator_identity,
            scalars: roots,
            output: output_columns,
            inputs: inputs.into(),
            cte,
            input_stats: shell.stats.clone(),
        };
        if maybe_cached {
            if let Some(entry) = self.locals.get(&key) {
                self.hits += 1;
                return Ok(entry.clone());
            }
        }
        self.misses += 1;
        let child_layouts = inputs
            .iter()
            .map(|id| self.facts[*id].layout.clone())
            .collect::<Vec<_>>();
        let mut plan = shell.assemble(
            inputs
                .iter()
                .enumerate()
                .map(|(ordinal, fact)| self.boundary(ordinal, *fact).map(Box::new))
                .collect::<Result<Vec<_>>>()?,
        )?;
        crate::expression::scalar_normalizer().visit_operator_expressions(&mut plan.operator);
        let mut context = crate::context::OptimizationContext::new(
            environment.session.clone(),
            environment.bind_context.clone(),
        );
        context.cost_model = environment.cost_model.clone();
        for fact in inputs.iter().map(|id| &self.facts[*id]) {
            for (binding, column) in fact.layout.bindings().iter().zip(&fact.columns) {
                context.column_stats_mut().insert(*binding, column.clone());
            }
        }
        let mut gathering = StatisticsGathering::new();
        if let Some((index, fact)) = cte {
            let fact = &self.facts[fact];
            gathering.bind_cte_domain(
                index,
                fact.stats.estimated_cardinality,
                fact.columns.clone(),
            );
            if let LogicalOperator::CTERef(reference) = &plan.operator {
                for (ordinal, column) in fact.columns.iter().enumerate() {
                    context.column_stats_mut().insert(
                        ColumnBinding::new(reference.table_index, ordinal),
                        column.clone(),
                    );
                }
            }
        }
        // Propagate this shell only. Input domains are immutable positional
        // snapshots, never recollected through another occurrence's bindings.
        let input_column_stats = context.column_stats.clone();
        let mut propagator =
            StatisticsPropagator::with_statistics_map(context.column_stats.as_ref().clone());
        plan = plan.map_operator(|operator| {
            propagator.propagate_operator(environment.session.as_ref(), operator)
        });
        context.column_stats = Arc::new(propagator.take_statistics_map());
        if matches!(&plan.operator, LogicalOperator::Filter(filter)
            if filter.expressions.is_empty()
                && filter.projection_map.is_identity(child_layouts[0].bindings().len()))
        {
            let LogicalOperator::Filter(filter) = plan.into_operator() else {
                unreachable!()
            };
            plan = *filter.child;
        }
        if let LogicalOperator::BoundReference(reference) = &plan.operator {
            // An identity filter can disappear during local propagation. Its
            // replacement is an input, not a new relation with unknown column
            // domains. Preserve the exact positional snapshot and hard bound.
            let ordinal = reference.reference_id.input_ordinal_value()?;
            let facts = *inputs.get(ordinal).ok_or_else(|| {
                paro_error::internal("settlement boundary reference is outside its input set")
            })?;
            let entry = SettledLocal {
                recipe: recipes.import(plan)?,
                facts,
                shape: shape.clone(),
                statistics: context.column_stats,
            };
            self.local_shapes.insert(shape);
            self.local_shape_entries
                .entry(entry.shape.clone())
                .or_default()
                .push(entry.clone());
            self.locals.insert(key, entry.clone());
            self.recipe_prefix = Some(recipes.checkpoint());
            return Ok(entry);
        }
        let (plan, output, maximum) = gathering.gather_local(
            plan,
            &child_layouts,
            &child_maxima,
            input_column_stats,
            &mut context,
        );
        if tracing::enabled!(target: "paro::optimizer::settlement", tracing::Level::TRACE) {
            let evidence = inputs
                .iter()
                .map(|id| {
                    let fact = &self.facts[*id];
                    (
                        *id,
                        fact.stats.estimated_cardinality,
                        fact.layout
                            .bindings()
                            .iter()
                            .zip(&fact.columns)
                            .map(|(binding, statistics)| (*binding, statistics.distinct_evidence()))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>();
            tracing::trace!(target: "paro::optimizer::settlement",
                operator = ?plan.operator.op_type(), scalar_roots = ?key.scalars, ?evidence,
                output = ?plan.stats.estimated_cardinality,
                "derived node-local cardinality");
        }
        // UNION inputs can deliberately have identical bindings (two domains
        // of the same producer). Merge their positional snapshots, not a map
        // in which the last visited branch overwrote the first.
        if let LogicalOperator::SetOperation(_) = &plan.operator {
            if inputs.len() == 2 {
                crate::statistics::gathering::merge_set_operation_column_statistics(
                    &output,
                    &self.facts[inputs[0]].columns,
                    &self.facts[inputs[1]].columns,
                    &mut context,
                );
            }
        }
        let facts = self.intern_fact(RelationFacts {
            columns: output
                .bindings()
                .iter()
                .zip(output.types())
                .map(|(binding, ty)| {
                    context
                        .column_stats
                        .get(binding)
                        .cloned()
                        .unwrap_or_else(|| ColumnStatistics::create_unknown(ty.clone()))
                })
                .collect(),
            layout: output,
            stats: plan.stats.clone(),
            maximum,
            column_ids: Box::new([]),
            binding_columns: None,
        })?;
        let entry = SettledLocal {
            recipe: recipes.import(plan)?,
            facts,
            shape: shape.clone(),
            statistics: context.column_stats,
        };
        self.local_shapes.insert(shape);
        self.local_shape_entries
            .entry(entry.shape.clone())
            .or_default()
            .push(entry.clone());
        self.locals.insert(key, entry.clone());
        self.recipe_prefix = Some(recipes.checkpoint());
        Ok(entry)
    }

    fn output_layout_matches<Child>(
        &self,
        operator: &LogicalOperator<Child>,
        inputs: &[FactId],
        cached: &LogicalOutputLayout,
    ) -> bool {
        let child_layouts = inputs
            .iter()
            .filter_map(|id| self.facts.get(*id).map(|fact| &fact.layout))
            .collect::<Vec<_>>();
        if child_layouts.len() != inputs.len() {
            return false;
        }
        operator.output_layout_from_child_refs(&child_layouts) == *cached
    }

    #[cfg(test)]
    fn settle(
        &mut self,
        plan: OwnedLogicalPlan,
        environment: &PlannerRuleEnvironment,
    ) -> Result<SettledExpression<OwnedLogicalPlan>> {
        let settled = self.settle_arena(plan, environment)?;
        Ok(SettledExpression {
            plan: self.test_arena.export(settled.plan)?,
            statistics: settled.statistics,
            scopes: settled.scopes,
        })
    }

    #[cfg(test)]
    pub(super) fn settle_arena(
        &mut self,
        plan: OwnedLogicalPlan,
        environment: &PlannerRuleEnvironment,
    ) -> Result<SettledExpression> {
        let mut arena = std::mem::take(&mut self.test_arena);
        let result = self.settle_arena_in(plan, environment, &mut arena);
        self.test_arena = arena;
        result?.ok_or_else(|| paro_error::internal("test settlement was interrupted"))
    }

    /// Settle into the session's sole storage owner. The result is an index,
    /// not a writable snapshot: subsequent alternatives cannot fork storage.
    pub(super) fn settle_arena_in(
        &mut self,
        plan: OwnedLogicalPlan,
        environment: &PlannerRuleEnvironment,
        arena: &mut LogicalPlanArena,
    ) -> Result<Option<SettledExpression>> {
        let checkpoint = arena.checkpoint();
        let result = self.settle_arena_impl(plan, environment, arena);
        if !matches!(result, Ok(Some(_))) {
            arena.rollback_to(checkpoint)?;
            self.discard_stale_recipes(arena);
        }
        result
    }

    fn settle_arena_impl(
        &mut self,
        plan: OwnedLogicalPlan,
        environment: &PlannerRuleEnvironment,
        arena: &mut LogicalPlanArena,
    ) -> Result<Option<SettledExpression>> {
        enum Task {
            Enter(PlanIndex, CteEnvironment),
            Consumer(PlanIndex, usize, CteEnvironment),
            Finish(PlanIndex, CteEnvironment, usize),
        }
        if !environment.control.checkpoint()? {
            return Ok(None);
        }
        let Some(root) = arena.import_controlled(plan, || {
            environment.session.cancellation.check()?;
            environment.control.checkpoint()
        })?
        else {
            return Ok(None);
        };
        let Some(demands) = demand::derive(arena, root, environment)? else {
            return Ok(None);
        };
        // Keep the imported source DAG and settled output in one arena.  The
        // source edges are immutable and remain valid while settled nodes are
        // appended, so settlement no longer allocates a second arena or
        // copies the complete input DAG before publishing the result.
        let mut pending = vec![Task::Enter(root, Arc::default())];
        let mut completed = Vec::<(
            PlanIndex,
            FactId,
            SharedColumnStatistics,
            demand::BindingMap,
        )>::new();
        let mut scopes = HashMap::new();
        while let Some(task) = pending.pop() {
            environment.session.cancellation.check()?;
            if !environment.control.checkpoint()? {
                return Ok(None);
            }
            match task {
                Task::Enter(index, ctes) => {
                    let node = arena.get(index)?;
                    let mut children = Vec::new();
                    node.operator
                        .visit_child_links(&mut |child| children.push(*child));
                    pending.push(Task::Finish(index, ctes.clone(), children.len()));
                    if let LogicalOperator::MaterializedCTE(cte) = &node.operator {
                        pending.push(Task::Consumer(cte.child, cte.cte_index, ctes.clone()));
                        pending.push(Task::Enter(cte.cte_query, ctes));
                    } else {
                        pending.extend(
                            children
                                .into_iter()
                                .rev()
                                .map(|child| Task::Enter(child, ctes.clone())),
                        );
                    }
                }
                Task::Consumer(index, cte, mut ctes) => {
                    let fact = completed
                        .last()
                        .ok_or_else(|| paro_error::internal("CTE settlement lost producer facts"))?
                        .1;
                    Arc::make_mut(&mut ctes).insert(cte, fact);
                    pending.push(Task::Enter(index, ctes));
                }
                Task::Finish(index, ctes, arity) => {
                    let mut node = arena.get(index)?.clone();
                    let start = completed
                        .len()
                        .checked_sub(arity)
                        .ok_or_else(|| paro_error::internal("settlement lost completed inputs"))?;
                    let children = completed.drain(start..).collect::<Vec<_>>();
                    if let LogicalOperator::BoundReference(reference) = &node.operator {
                        node.stats.estimated_cardinality = reference.facts.cardinality;
                        node.stats.unique_keys = reference.facts.unique_keys.clone();
                        let output = node.operator.output_layout_from_children(&[]);
                        let facts = self.intern_fact(RelationFacts {
                            layout: output,
                            stats: node.stats.clone(),
                            maximum: reference.facts.maximum_cardinality,
                            columns: reference.column_statistics(),
                            column_ids: Box::new([]),
                            binding_columns: None,
                        })?;
                        let statistics: SharedColumnStatistics = Arc::new(
                            reference
                                .bindings
                                .iter()
                                .copied()
                                .zip(reference.column_statistics())
                                .collect(),
                        );
                        // Synthetic ids are intentionally non-unique and
                        // cannot serve as scope-cache keys. Settlement is
                        // the ownership boundary, so mint a real occurrence
                        // id before publishing this scope.
                        if node.id.is_synthetic() {
                            node.id = environment.bind_context.next_plan_id();
                        }
                        scopes.insert(node.id, statistics.clone());
                        let aliases = reference
                            .bindings
                            .iter()
                            .map(|binding| (*binding, *binding))
                            .collect();
                        completed.push((arena.append(node)?, facts, statistics, aliases));
                        continue;
                    }
                    let mut before = Vec::new();
                    node.operator.visit_child_links(&mut |child| {
                        before.push(demands.layouts[child].clone())
                    });
                    let shell = LogicalPlanNode {
                        id: node.id,
                        stats: node.stats,
                        operator: node.operator.try_map_child_links(&mut |_| {
                            Ok::<_, paro_common::error::ParoError>(())
                        })?,
                    };
                    let (shell, aliases) = demand::apply(
                        shell,
                        demand::Inputs {
                            old_carriers: &demands.carriers[&index],
                            before: &before,
                            after: &children
                                .iter()
                                .map(|child| self.facts[child.1].layout.clone())
                                .collect::<Vec<_>>(),
                            children: &children
                                .iter()
                                .map(|child| child.3.clone())
                                .collect::<Vec<_>>(),
                        },
                        &demands.outputs[&index],
                        &mut self.scan_bindings,
                        &environment.bind_context,
                    )?;
                    let local = self.local(
                        shell,
                        &children
                            .iter()
                            .map(|(_, fact, _, _)| *fact)
                            .collect::<Vec<_>>(),
                        &ctes,
                        environment,
                        arena,
                    )?;
                    let mut remapped = BTreeMap::new();
                    for recipe_index in arena.post_order(local.recipe)? {
                        let recipe = arena.get(recipe_index)?.clone();
                        if let LogicalOperator::BoundReference(reference) = &recipe.operator {
                            let ordinal = reference.reference_id.input_ordinal_value()?;
                            let child = children.get(ordinal).ok_or_else(|| {
                                paro_error::internal("settlement recipe changed its input contract")
                            })?;
                            remapped.insert(recipe_index, child.0);
                            continue;
                        }
                        let operator = recipe.operator.try_map_child_links(&mut |child| {
                            remapped.get(&child).copied().ok_or_else(|| {
                                paro_error::internal("settlement recipe lost an input")
                            })
                        })?;
                        let id = environment.bind_context.next_plan_id();
                        scopes.insert(id, local.statistics.clone());
                        let mapped = arena.append(LogicalPlanNode {
                            id,
                            stats: recipe.stats,
                            operator,
                        })?;
                        remapped.insert(recipe_index, mapped);
                    }
                    completed.push((
                        remapped[&local.recipe],
                        local.facts,
                        local.statistics,
                        aliases,
                    ));
                }
            }
        }
        if completed.len() != 1 {
            return Err(paro_error::internal("settlement has no unique result"));
        }
        let (root, _, statistics, _) = completed.pop().unwrap();
        Ok(Some(SettledExpression {
            plan: root,
            statistics,
            scopes,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paro_common::runtime_value::Value;
    use paro_context::TestStatementContextBuilder;
    use paro_planner::binder::ir::CTEMaterialize;
    use paro_planner::expression::{ColumnRefExpression, ConstantExpression};
    use paro_planner::operator::{CTERef, ExpressionGet, MaterializedCTE, Projection};

    fn environment() -> PlannerRuleEnvironment {
        let bind_context = BindContext::new();
        for _ in 0..16 {
            bind_context.generate_table_index();
        }
        PlannerRuleEnvironment {
            control: Arc::new(crate::cascades::control::SearchControl::new(None)),
            bind_context,
            session: TestStatementContextBuilder::minimal().build(),
            cost_model: Default::default(),
            budget: SearchBudget::default(),
            verify_enabled: false,
        }
    }

    fn values(bind: &BindContext, rows: usize) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
            bind,
            LogicalOperator::ExpressionGet(ExpressionGet::new(
                0,
                (0..rows)
                    .map(|row| {
                        vec![Expression::Constant(
                            ConstantExpression::new(
                                Value::Integer(row as i32),
                                LogicalType::Integer,
                            )
                            .into(),
                        )]
                    })
                    .collect(),
                vec!["key".into()],
                vec![LogicalType::Integer],
            )),
        )
    }

    fn project(bind: &BindContext, child: OwnedLogicalPlan) -> OwnedLogicalPlan {
        OwnedLogicalPlan::new(
            bind,
            LogicalOperator::Projection(Projection::new(
                1,
                child,
                vec![Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer).into(),
                )],
            )),
        )
    }

    #[test]
    fn limits_and_distinct_targets_are_not_scalar_free_cache_keys() {
        let env = environment();
        let literal = |value| {
            Expression::Constant(
                ConstantExpression::new(Value::BigInt(value), LogicalType::BigInt).into(),
            )
        };
        let limit = |value| {
            OwnedLogicalPlan::new(
                &env.bind_context,
                LogicalOperator::Limit(Box::new(paro_planner::operator::Limit::new(
                    values(&env.bind_context, 8),
                    Some(literal(value)),
                    None,
                ))),
            )
        };
        assert!(!operator_has_no_scalar_payload(&limit(2).operator));
        let distinct = LogicalOperator::Distinct(paro_planner::operator::Distinct::distinct_on(
            vec![literal(1)],
            values(&env.bind_context, 8),
        ));
        assert!(!operator_has_no_scalar_payload(&distinct));
        let mut cache = SettlementCache::default();
        let first = cache.settle(limit(2), &env).unwrap();
        let second = cache.settle(limit(5), &env).unwrap();
        let bound = |plan: &OwnedLogicalPlan| {
            let LogicalOperator::Limit(limit) = &plan.operator else {
                panic!("expected limit");
            };
            let Some(Expression::Constant(constant)) = &limit.limit else {
                panic!("expected constant");
            };
            constant.value.clone()
        };
        assert_ne!(bound(&first.plan), bound(&second.plan));
    }

    #[test]
    fn limit_and_offset_keep_their_optional_operand_positions_in_cache_identity() {
        let env = environment();
        let value = Expression::Constant(
            ConstantExpression::new(Value::BigInt(2), LogicalType::BigInt).into(),
        );
        let make = |limit, offset| {
            OwnedLogicalPlan::new(
                &env.bind_context,
                LogicalOperator::Limit(Box::new(paro_planner::operator::Limit::new(
                    values(&env.bind_context, 8),
                    limit,
                    offset,
                ))),
            )
        };
        let mut cache = SettlementCache::default();
        let limited = cache.settle(make(Some(value.clone()), None), &env).unwrap();
        let skipped = cache.settle(make(None, Some(value)), &env).unwrap();
        let LogicalOperator::Limit(limited) = &limited.plan.operator else {
            panic!("LIMIT")
        };
        let LogicalOperator::Limit(skipped) = &skipped.plan.operator else {
            panic!("OFFSET")
        };
        assert!(limited.limit.is_some() && limited.offset.is_none());
        assert!(skipped.limit.is_none() && skipped.offset.is_some());
    }

    #[test]
    fn a_local_boundary_preserves_expected_ndv_without_inventing_a_hard_bound() {
        let mut cache = SettlementCache::default();
        let facts = cache
            .intern_fact(RelationFacts {
                layout: LogicalOutputLayout::new(
                    vec![LogicalType::Integer],
                    vec![ColumnBinding::new(0, 0)],
                ),
                stats: NodeStats {
                    estimated_cardinality: Some(CardinalityEstimate::exact(100)),
                    ..NodeStats::default()
                },
                maximum: None,
                columns: vec![Arc::new(ColumnStatistics::with_estimated_distinct(
                    paro_storage::statistics::BaseStatistics::create_unknown(LogicalType::Integer),
                    Some(10_000),
                ))],
                column_ids: Box::new([]),
                binding_columns: None,
            })
            .unwrap();
        assert_eq!(
            cache.facts[facts].columns[0].distinct_evidence().point,
            10_000
        );
        assert_eq!(
            cache.facts[facts].columns[0].guaranteed_distinct_upper(),
            None
        );
        let boundary = cache.boundary(0, facts).unwrap();
        let LogicalOperator::BoundReference(reference) = &boundary.operator else {
            panic!("not a boundary")
        };
        assert_eq!(
            reference.facts.column_domains[0].expected_distinct,
            Some(10_000)
        );
        assert_eq!(
            reference.facts.column_domains[0].guaranteed_distinct_upper,
            None
        );
    }

    #[test]
    fn uncertain_row_point_does_not_destroy_an_observed_domain() {
        let mut cache = SettlementCache::default();
        let facts = cache
            .intern_fact(RelationFacts {
                layout: LogicalOutputLayout::new(
                    vec![LogicalType::Integer],
                    vec![ColumnBinding::new(0, 0)],
                ),
                stats: NodeStats {
                    estimated_cardinality: Some(CardinalityEstimate {
                        min: 0,
                        expected: 1,
                        max: 100,
                    }),
                    ..NodeStats::default()
                },
                maximum: None,
                columns: vec![Arc::new(ColumnStatistics::with_estimated_distinct(
                    paro_storage::statistics::BaseStatistics::create_unknown(LogicalType::Integer),
                    Some(10),
                ))],
                column_ids: Box::new([]),
                binding_columns: None,
            })
            .unwrap();

        assert_eq!(
            cache.facts[facts].columns[0].distinct_evidence().point,
            10,
            "the expected row point is not a hard NDV proof"
        );
        let boundary = cache.boundary(0, facts).unwrap();
        let LogicalOperator::BoundReference(reference) = &boundary.operator else {
            panic!("not a boundary")
        };
        assert_eq!(
            reference.facts.column_domains[0].expected_distinct,
            Some(10)
        );
    }

    #[test]
    fn unchanged_input_facts_reuse_local_recipes_with_fresh_occurrences() {
        let env = environment();
        let mut cache = SettlementCache::default();
        let first = cache
            .settle(
                project(&env.bind_context, values(&env.bind_context, 3)),
                &env,
            )
            .unwrap();
        let misses = cache.misses;
        let input_column_arrays = cache
            .facts
            .iter()
            .filter_map(|fact| {
                fact.binding_columns
                    .as_ref()
                    .map(|columns| (columns.as_ptr(), columns.to_vec()))
            })
            .collect::<Vec<_>>();
        let native_column_count = cache.columns.len();
        assert_eq!(cache.input_column_cache_misses, 1);
        let second = cache
            .settle(
                project(&env.bind_context, values(&env.bind_context, 3)),
                &env,
            )
            .unwrap();
        assert_eq!(cache.misses, misses);
        assert_eq!(cache.hits, 2);
        assert_eq!(cache.input_column_cache_misses, 1);
        assert_eq!(cache.input_column_cache_hits, 1);
        assert_eq!(cache.columns.len(), native_column_count);
        assert_eq!(
            cache
                .facts
                .iter()
                .filter_map(|fact| {
                    fact.binding_columns
                        .as_ref()
                        .map(|columns| (columns.as_ptr(), columns.to_vec()))
                })
                .collect::<Vec<_>>(),
            input_column_arrays
        );
        assert_ne!(first.plan.id, second.plan.id);
        assert_eq!(first.plan.stats, second.plan.stats);
        assert_eq!(second.plan.stats.estimated_cardinality.unwrap().expected, 3);
    }

    #[test]
    fn rollback_of_unrelated_slots_does_not_walk_the_settlement_cache() {
        let environment = environment();
        let mut cache = SettlementCache::default();
        let mut arena = LogicalPlanArena::default();
        let empty = arena.checkpoint();
        let first = cache
            .settle_arena_in(
                project(
                    &environment.bind_context,
                    values(&environment.bind_context, 4),
                ),
                &environment,
                &mut arena,
            )
            .unwrap()
            .unwrap();
        let retained = arena.checkpoint();
        let native_columns = cache.columns.len();
        let interned_layouts = cache.input_column_cache_misses;
        assert!(!cache.locals.is_empty());
        for _ in 0..1000 {
            arena
                .append(LogicalPlanNode {
                    id: PlanNodeId::SYNTHETIC,
                    stats: NodeStats::default(),
                    operator: LogicalOperator::DummyScan,
                })
                .unwrap();
            arena.rollback_to(retained).unwrap();
            cache.discard_stale_recipes(&arena);
            assert!(arena.owns(first.plan));
        }
        assert_eq!(cache.invalidation_visits, 0);
        arena.rollback_to(empty).unwrap();
        cache.discard_stale_recipes(&arena);
        assert!(cache.invalidation_visits > 0);
        assert!(cache.locals.is_empty());
        let next = cache
            .settle_arena_in(
                project(
                    &environment.bind_context,
                    values(&environment.bind_context, 4),
                ),
                &environment,
                &mut arena,
            )
            .unwrap()
            .unwrap();
        assert!(!arena.owns(first.plan));
        assert!(arena.owns(next.plan));
        assert_eq!(cache.columns.len(), native_columns);
        assert_eq!(
            cache.input_column_cache_misses, interned_layouts,
            "recipe rollback must not invalidate the immutable fact/column namespace"
        );
    }

    #[test]
    fn column_fact_interning_keeps_distinct_proof_provenance_separate() {
        let hashes: Vec<u64> = (0..32u64)
            .map(|value| value.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .collect();
        let mut full = ColumnStatistics::new(
            paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
        )
        .with_storage_observation();
        full.update_distinct_statistics(&hashes, hashes.len());

        let mut partial = full.copy();
        let unknown = ColumnStatistics::with_distinct(
            paro_storage::statistics::BaseStatistics::create_empty(LogicalType::Integer),
            None,
        );
        partial.merge_with_coverage(&unknown, hashes.len() as u64, 128);
        partial = partial.with_storage_observation();

        assert_ne!(
            full.distinct_evidence(),
            partial.distinct_evidence(),
            "partial coverage is a different proof contract even when the sketch bytes match"
        );

        let mut cache = SettlementCache::default();
        let full_id = cache.intern_column_value(&Arc::new(full)).unwrap();
        let partial_id = cache.intern_column_value(&Arc::new(partial)).unwrap();
        assert_ne!(full_id, partial_id);
    }

    #[test]
    fn grouped_sum_estimate_crosses_group_holes_and_renaming_without_a_tree_witness() {
        use paro_function::aggregate::distributive::sum::get_sum_function;
        use paro_planner::expression::{AggregateExpression, ComparisonExpression, ComparisonType};
        use paro_planner::operator::{Aggregate, Filter};
        use paro_storage::statistics::{BaseStatistics, NumericStats};

        let env = environment();
        let mut cache = SettlementCache::default();
        let mut amount = BaseStatistics::create_empty(LogicalType::Integer);
        NumericStats::update(&mut amount, &Value::Integer(0));
        NumericStats::update(&mut amount, &Value::Integer(200));
        let input = cache
            .intern_fact(RelationFacts {
                layout: LogicalOutputLayout::new(
                    vec![LogicalType::Integer; 2],
                    vec![ColumnBinding::new(0, 0), ColumnBinding::new(0, 1)],
                ),
                stats: NodeStats {
                    estimated_cardinality: Some(CardinalityEstimate::exact(8)),
                    ..NodeStats::default()
                },
                maximum: None,
                columns: vec![
                    Arc::new(ColumnStatistics::with_estimated_distinct(
                        BaseStatistics::create_unknown(LogicalType::Integer),
                        Some(4),
                    )),
                    Arc::new(ColumnStatistics::with_estimated_distinct(amount, Some(6))),
                ],
                column_ids: Box::new([]),
                binding_columns: None,
            })
            .unwrap();
        let column = |table, index, ty| {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(table, index), ty).into(),
            )
        };
        let (sum, _) = get_sum_function().bind(&[LogicalType::Integer]).unwrap();
        let aggregate = OwnedLogicalPlan::new(
            &env.bind_context,
            LogicalOperator::Aggregate(Box::new(Aggregate::new(
                1,
                2,
                3,
                cache.boundary(0, input).unwrap(),
                vec![column(0, 0, LogicalType::Integer)],
                Vec::new(),
                vec![Expression::Aggregate(
                    AggregateExpression::new(
                        sum,
                        vec![column(0, 1, LogicalType::Integer)],
                        LogicalType::BigInt,
                    )
                    .into(),
                )],
                Vec::new(),
            ))),
        );
        let settled = cache.settle(aggregate, &env).unwrap();
        let amount = settled.statistics[&ColumnBinding::new(2, 0)].clone();
        assert_eq!(
            amount.estimated_numeric_distribution().unwrap().mean(),
            200.0
        );
        let output = cache
            .intern_fact(RelationFacts {
                layout: settled.plan.output_layout(),
                stats: settled.plan.stats.clone(),
                maximum: None,
                columns: vec![
                    settled.statistics[&ColumnBinding::new(1, 0)].clone(),
                    amount,
                ],
                column_ids: Box::new([]),
                binding_columns: None,
            })
            .unwrap();
        for renamed in [false, true] {
            // No Aggregate exists in either consumer's bound input. The
            // selectivity must come exclusively from the column fact.
            let boundary = cache.boundary(0, output).unwrap();
            let (child, amount) = if renamed {
                (
                    OwnedLogicalPlan::new(
                        &env.bind_context,
                        LogicalOperator::Projection(Projection::new(
                            4,
                            boundary,
                            vec![
                                column(1, 0, LogicalType::Integer),
                                column(2, 0, LogicalType::BigInt),
                            ],
                        )),
                    ),
                    column(4, 1, LogicalType::BigInt),
                )
            } else {
                (boundary, column(2, 0, LogicalType::BigInt))
            };
            let filter = OwnedLogicalPlan::new(
                &env.bind_context,
                LogicalOperator::Filter(Filter::new(
                    child,
                    vec![Expression::Comparison(
                        ComparisonExpression::new(
                            ComparisonType::GreaterThan,
                            amount,
                            Expression::Constant(
                                ConstantExpression::new(Value::BigInt(100), LogicalType::BigInt)
                                    .into(),
                            ),
                        )
                        .into(),
                    )],
                )),
            );
            let output = cache.settle(filter, &env).unwrap();
            assert_eq!(output.plan.stats.estimated_cardinality.unwrap().expected, 3);
            let amount = ColumnBinding::new(if renamed { 4 } else { 2 }, usize::from(renamed));
            assert_eq!(
                output.statistics[&amount].estimated_numeric_distribution(),
                None,
                "a filtered output cannot republish its unconditional input distribution"
            );
        }
    }

    #[test]
    fn filter_estimates_input_domain_before_publishing_output_proof() {
        use paro_planner::expression::{ComparisonExpression, ComparisonType};
        use paro_planner::operator::Filter;
        use paro_storage::statistics::BaseStatistics;

        let env = environment();
        let mut cache = SettlementCache::default();
        let binding = ColumnBinding::new(0, 0);
        let input = cache
            .intern_fact(RelationFacts {
                layout: LogicalOutputLayout::new(vec![LogicalType::Integer], vec![binding]),
                stats: NodeStats {
                    estimated_cardinality: Some(CardinalityEstimate::exact(5)),
                    ..NodeStats::default()
                },
                maximum: None,
                columns: vec![Arc::new(ColumnStatistics::with_estimated_distinct(
                    BaseStatistics::create_unknown(LogicalType::Integer),
                    Some(3),
                ))],
                column_ids: Box::new([]),
                binding_columns: None,
            })
            .unwrap();
        let filter = OwnedLogicalPlan::new(
            &env.bind_context,
            LogicalOperator::Filter(Filter::new(
                cache.boundary(0, input).unwrap(),
                vec![Expression::Comparison(
                    ComparisonExpression::new(
                        ComparisonType::Equal,
                        Expression::ColumnRef(
                            ColumnRefExpression::new(binding, LogicalType::Integer).into(),
                        ),
                        Expression::Constant(
                            ConstantExpression::new(Value::Integer(1), LogicalType::Integer).into(),
                        ),
                    )
                    .into(),
                )],
            )),
        );
        let shell = LogicalPlanNode::from_shell(filter);
        let mut arena = LogicalPlanArena::default();
        let ctes = CteEnvironment::default();
        for _ in 0..2 {
            let result = cache
                .local(shell.clone(), &[input], &ctes, &env, &mut arena)
                .unwrap();
            let facts = &cache.facts[result.facts];
            assert_eq!(facts.stats.estimated_cardinality.unwrap().expected, 2);
            assert_eq!(facts.columns[0].distinct_evidence().point, 1);
            assert_eq!(facts.columns[0].guaranteed_distinct_upper(), Some(1));
            assert_eq!(cache.facts[input].columns[0].distinct_evidence().point, 3);
        }
        assert_eq!(
            cache.hits, 1,
            "cache replay preserves the same input/output contract"
        );
    }

    #[test]
    fn eliminating_identity_filter_preserves_exact_input_facts() {
        let env = environment();
        let mut cache = SettlementCache::default();
        let source = cache.settle(values(&env.bind_context, 3), &env).unwrap();
        let expected = source.statistics[&ColumnBinding::new(0, 0)].clone();
        let filtered = OwnedLogicalPlan::new(
            &env.bind_context,
            LogicalOperator::Filter(paro_planner::operator::Filter::new(
                values(&env.bind_context, 3),
                vec![Expression::Constant(
                    ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean).into(),
                )],
            )),
        );
        let actual = cache.settle(filtered, &env).unwrap();
        assert!(matches!(
            actual.plan.operator,
            LogicalOperator::ExpressionGet(_)
        ));
        assert_eq!(actual.plan.stats, source.plan.stats);
        assert_eq!(
            actual.statistics[&ColumnBinding::new(0, 0)]
                .to_bytes()
                .unwrap(),
            expected.to_bytes().unwrap()
        );
        assert_eq!(
            actual.statistics[&ColumnBinding::new(0, 0)].guaranteed_distinct_upper(),
            expected.guaranteed_distinct_upper()
        );
    }

    #[test]
    fn new_parent_demand_widens_a_local_filter_without_reinterpreting_scan_bindings() {
        use paro_planner::expression::{ComparisonExpression, ComparisonType};
        use paro_planner::operator::{Filter, Get, ProjectionMap};
        let env = environment();
        let scan = OwnedLogicalPlan::new(
            &env.bind_context,
            LogicalOperator::Get(Box::new(Get::new_without_table(
                0,
                vec!["a".into(), "b".into(), "c".into()],
                vec![LogicalType::Integer; 3],
            ))),
        );
        let mut filter = Filter::new(
            scan,
            vec![Expression::Comparison(
                ComparisonExpression::new(
                    ComparisonType::GreaterThan,
                    Expression::ColumnRef(
                        ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::Integer)
                            .into(),
                    ),
                    Expression::Constant(
                        ConstantExpression::new(Value::Integer(0), LogicalType::Integer).into(),
                    ),
                )
                .into(),
            )],
        );
        filter.projection_map = ProjectionMap::new(vec![0]);
        let plan = OwnedLogicalPlan::new(
            &env.bind_context,
            LogicalOperator::Projection(Projection::new(
                1,
                OwnedLogicalPlan::new(&env.bind_context, LogicalOperator::Filter(filter)),
                vec![Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 2), LogicalType::Integer).into(),
                )],
            )),
        );
        let mut cache = SettlementCache::default();
        let result = cache.settle(plan, &env).unwrap();
        let LogicalOperator::Projection(projection) = &result.plan.operator else {
            panic!()
        };
        let LogicalOperator::Filter(filter) = &projection.child.operator else {
            panic!()
        };
        let LogicalOperator::Get(scan) = &filter.child.operator else {
            panic!()
        };
        assert_ne!(
            scan.table_index, 0,
            "Memo column identities cannot be reassigned to a different catalog column"
        );
        assert_eq!(
            scan.returned_types.len(),
            2,
            "predicate-only a is retained, unused b is not"
        );
        assert_eq!(scan.stored_column(0), Some(0));
        assert_eq!(scan.stored_column(1), Some(2));
        assert_eq!(filter.projection_map.as_columns(), Some([1].as_slice()));
        let Expression::ColumnRef(column) = &projection.expressions[0] else {
            panic!()
        };
        assert_eq!(column.binding, ColumnBinding::new(scan.table_index, 1));
    }

    #[test]
    fn cte_reference_reads_its_lexical_producer_not_a_sibling_snapshot() {
        let env = environment();
        let mut cache = SettlementCache::default();
        let owner = |rows| {
            OwnedLogicalPlan::new(
                &env.bind_context,
                LogicalOperator::MaterializedCTE(MaterializedCTE::new(
                    2,
                    "shared".into(),
                    vec!["key".into()],
                    vec![LogicalType::Integer],
                    CTEMaterialize::Default,
                    values(&env.bind_context, rows),
                    OwnedLogicalPlan::new(
                        &env.bind_context,
                        LogicalOperator::CTERef(CTERef::new(
                            2,
                            3,
                            "shared".into(),
                            vec!["key".into()],
                            vec![LogicalType::Integer],
                        )),
                    ),
                )),
            )
        };
        for rows in [3, 7, 3] {
            let settled = cache.settle(owner(rows), &env).unwrap();
            let LogicalOperator::MaterializedCTE(cte) = &settled.plan.operator else {
                panic!("CTE owner changed")
            };
            assert_eq!(
                cte.child.stats.estimated_cardinality.unwrap().expected,
                rows as u64
            );
        }
        assert!(
            cache.hits >= 3,
            "the original fact environment should be reusable"
        );
    }

    #[test]
    fn storage_encoding_does_not_erase_plan_local_domain_proofs_from_cache_keys() {
        let mut cache = SettlementCache::default();
        let fact = |upper| RelationFacts {
            layout: LogicalOutputLayout::new(
                vec![LogicalType::Integer],
                vec![ColumnBinding::new(0, 0)],
            ),
            stats: NodeStats::default(),
            maximum: None,
            columns: vec![Arc::new(
                ColumnStatistics::new(paro_storage::statistics::BaseStatistics::create_unknown(
                    LogicalType::Integer,
                ))
                .with_guaranteed_distinct_upper(upper),
            )],
            column_ids: Box::new([]),
            binding_columns: None,
        };
        let a = cache.intern_fact(fact(3)).unwrap();
        let b = cache.intern_fact(fact(7)).unwrap();
        assert_ne!(a, b);
        assert_eq!(a, cache.intern_fact(fact(3)).unwrap());
    }

    #[test]
    fn local_boundary_preserves_ndv_values_and_hard_row_bound_without_an_hll() {
        let mut cache = SettlementCache::default();
        let fact = |ndv| RelationFacts {
            layout: LogicalOutputLayout::new(
                vec![LogicalType::Integer],
                vec![ColumnBinding::new(0, 0)],
            ),
            stats: NodeStats {
                estimated_cardinality: Some(CardinalityEstimate::exact(100)),
                ..Default::default()
            },
            maximum: Some(200),
            columns: vec![Arc::new(
                ColumnStatistics::with_estimated_distinct(
                    paro_storage::statistics::BaseStatistics::create_unknown(LogicalType::Integer),
                    Some(ndv),
                )
                .with_guaranteed_distinct_upper(90),
            )],
            column_ids: Box::new([]),
            binding_columns: None,
        };
        let first = cache.intern_fact(fact(30)).unwrap();
        let second = cache.intern_fact(fact(70)).unwrap();
        assert_ne!(
            first, second,
            "stored statistics encoding omits this planner estimate"
        );
        let LogicalOperator::BoundReference(reference) =
            cache.boundary(0, first).unwrap().into_operator()
        else {
            panic!()
        };
        let statistics = reference.column_statistics();
        assert_eq!(statistics[0].distinct_evidence().point, 30);
        assert_eq!(statistics[0].guaranteed_distinct_upper(), Some(90));
        assert!(!statistics[0].has_distinct_stats());
        assert_eq!(reference.facts.maximum_cardinality, Some(200));
    }
}
