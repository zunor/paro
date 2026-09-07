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

mod demand;

type FactId = usize;
type CteEnvironment = Arc<BTreeMap<usize, FactId>>;

#[derive(Debug)]
struct RelationFacts {
    layout: LogicalOutputLayout,
    stats: NodeStats,
    maximum: Option<u64>,
    columns: Vec<Arc<ColumnStatistics>>,
    column_ids: Box<[usize]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LocalKey {
    operator: Box<[u8]>,
    scalars: Box<[ScalarExprId]>,
    output: Box<[ColumnId]>,
    inputs: Box<[FactId]>,
    cte: Option<(usize, FactId)>,
}

#[derive(Debug, Clone)]
struct SettledLocal {
    input_stats: NodeStats,
    recipe: PlanIndex,
    facts: FactId,
    /// Statistics needed by this operator's expressions, including columns
    /// which are consumed locally but not returned by its projection map.
    statistics: SharedColumnStatistics,
}

#[derive(Debug, Default)]
pub(in crate::cascades::planner) struct SettlementCache {
    columns: ColumnCatalog,
    bindings: BindingCatalog,
    scalars: ScalarArena,
    recipes: LogicalPlanArena,
    locals: BTreeMap<LocalKey, Vec<SettledLocal>>,
    facts: Vec<RelationFacts>,
    facts_by_columns: BTreeMap<Box<[usize]>, Vec<FactId>>,
    column_values: HashMap<(LogicalType, Vec<u8>, usize, Option<u64>), usize>,
    column_pointers: HashMap<usize, (Arc<ColumnStatistics>, usize)>,
    scan_bindings: demand::ScanBindings,
    pub(in crate::cascades::planner) hits: u64,
    pub(in crate::cascades::planner) misses: u64,
}

pub(super) struct SettledExpression<Plan = paro_planner::plan::LogicalPlan> {
    pub(super) plan: Plan,
    pub(super) statistics: SharedColumnStatistics,
    pub(super) scopes: HashMap<PlanNodeId, SharedColumnStatistics>,
}

impl SettlementCache {
    fn intern_columns(&mut self, layout: &LogicalOutputLayout) -> Result<Box<[ColumnId]>> {
        layout
            .bindings()
            .iter()
            .zip(layout.types())
            .map(|(binding, ty)| {
                if let Some(id) = self
                    .bindings
                    .get(binding.table_index, binding.column_index, ty)
                {
                    return Ok(*id);
                }
                // This private arena's exact binding interner owns the identity.
                // The ordinal is never used as a semantic or scheduling priority.
                let column = self.columns.intern(
                    ty.clone(),
                    true,
                    ColumnOrigin::Internal {
                        key: Fingerprint(self.columns.len() as u128),
                    },
                    ColumnVisibility::Visible,
                    None,
                )?;
                self.bindings
                    .insert(binding.table_index, binding.column_index, ty, column)?;
                Ok(column)
            })
            .collect()
    }

    fn intern_fact(&mut self, mut fact: RelationFacts) -> Result<FactId> {
        // Every consumer of this boundary, including expression costing and
        // join estimation through the local statistics map, sees the same
        // row-domain estimate. Capping only BoundReference transport would
        // leave those direct readers with the pre-filter storage NDV.
        if let Some(rows) = fact.stats.estimated_cardinality {
            for column in &mut fact.columns {
                if column.get_distinct_count() as u64 > rows.expected {
                    let mut bounded = ColumnStatistics::with_estimated_distinct(
                        column.statistics().clone(),
                        Some(rows.expected as usize),
                    );
                    if let Some(upper) = column.guaranteed_distinct_upper() {
                        bounded = bounded.with_guaranteed_distinct_upper(upper);
                    }
                    *column = Arc::new(bounded);
                }
            }
        }
        let mut columns = Vec::with_capacity(fact.columns.len());
        for column in &fact.columns {
            let pointer = Arc::as_ptr(column) as usize;
            let id = if let Some((_, id)) = self.column_pointers.get(&pointer) {
                *id
            } else {
                let key = (
                    column.statistics().get_type().clone(),
                    column.to_bytes()?,
                    column.get_distinct_count(),
                    column.guaranteed_distinct_upper(),
                );
                let next = self.column_values.len();
                let id = *self.column_values.entry(key).or_insert(next);
                // Retain the Arc so address reuse can never validate a new
                // allocation against an old fact.
                self.column_pointers.insert(pointer, (column.clone(), id));
                id
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

    fn boundary(&self, ordinal: usize, fact: FactId) -> Result<OwnedLogicalPlan> {
        let fact = &self.facts[fact];
        let mut reference = BoundReference::new(
            ordinal as u32,
            fact.layout.bindings().to_vec(),
            fact.layout.types().to_vec(),
        );
        let mut domain = paro_planner::operator::bound_reference::BoundRelationFacts::default();
        domain.cardinality = fact.stats.estimated_cardinality;
        domain.maximum_cardinality = fact.maximum;
        domain.unique_keys = fact.stats.unique_keys.clone();
        domain.column_domains = fact
            .columns
            .iter()
            .map(
                |column| paro_planner::operator::bound_reference::BoundColumnDomain {
                    expected_distinct: u64::try_from(column.get_distinct_count())
                        .ok()
                        .filter(|value| *value > 0)
                        .map(|distinct| {
                            fact.stats
                                .estimated_cardinality
                                .map_or(distinct, |rows| distinct.min(rows.expected))
                        }),
                    guaranteed_distinct_upper: column.guaranteed_distinct_upper(),
                },
            )
            .collect();
        domain.column_values = fact
            .columns
            .iter()
            .map(|column| {
                paro_planner::operator::bound_reference::BoundColumnValues::new(
                    column.statistics().clone(),
                )
                .map(Some)
            })
            .collect::<Result<Vec<_>>>()?;
        reference.facts = Arc::new(domain);
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
    ) -> Result<SettledLocal> {
        let child_layouts = inputs
            .iter()
            .map(|id| self.facts[*id].layout.clone())
            .collect::<Vec<_>>();
        let child_maxima = inputs
            .iter()
            .map(|id| self.facts[*id].maximum)
            .collect::<Vec<_>>();
        let output = shell.operator.output_layout_from_children(&child_layouts);
        let output_columns = self.intern_columns(&output)?;
        let child_columns = child_layouts
            .iter()
            .map(|layout| self.intern_columns(layout))
            .collect::<Result<Vec<_>>>()?;
        let mut operator = shell.operator.clone();
        let roots = intern_operator_scalars(
            &mut operator,
            &output_columns,
            &child_columns,
            &mut self.bindings,
            &mut self.columns,
            &mut self.scalars,
        )?;
        let cte = if let LogicalOperator::CTERef(reference) = &operator {
            ctes.get(&reference.cte_index)
                .map(|fact| (reference.cte_index, *fact))
        } else {
            None
        };
        let key = LocalKey {
            operator: query_operator_identity(&operator, &roots, &self.scalars)?.1,
            scalars: roots,
            output: output_columns,
            inputs: inputs.into(),
            cte,
        };
        if let Some(entry) = self.locals.get(&key).and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.input_stats == shell.stats)
        }) {
            self.hits += 1;
            return Ok(entry.clone());
        }
        self.misses += 1;
        let input_stats = shell.stats.clone();
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
            let facts = inputs[reference.reference_id as usize];
            let entry = SettledLocal {
                input_stats,
                recipe: self.recipes.import(plan)?,
                facts,
                statistics: context.column_stats,
            };
            self.locals.entry(key).or_default().push(entry.clone());
            return Ok(entry);
        }
        let (plan, output, maximum) =
            gathering.gather_local(plan, &child_layouts, &child_maxima, &mut context);
        // UNION inputs can deliberately have identical bindings (two domains
        // of the same producer). Merge their positional snapshots, not a map
        // in which the last visited branch overwrote the first.
        if let LogicalOperator::SetOperation(_) = &plan.operator {
            if inputs.len() == 2 {
                for (ordinal, binding) in output.bindings().iter().enumerate() {
                    if let (Some(left), Some(right)) = (
                        self.facts[inputs[0]].columns.get(ordinal),
                        self.facts[inputs[1]].columns.get(ordinal),
                    ) {
                        let mut merged = left.as_ref().copy();
                        merged.merge(right.as_ref());
                        context
                            .column_stats_mut()
                            .insert(*binding, Arc::new(merged));
                    }
                }
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
        })?;
        let entry = SettledLocal {
            input_stats,
            recipe: self.recipes.import(plan)?,
            facts,
            statistics: context.column_stats,
        };
        self.locals.entry(key).or_default().push(entry.clone());
        Ok(entry)
    }

    #[cfg(test)]
    fn settle(
        &mut self,
        plan: OwnedLogicalPlan,
        environment: &PlannerRuleEnvironment,
    ) -> Result<SettledExpression<OwnedLogicalPlan>> {
        let settled = self.settle_arena(plan, environment)?;
        Ok(SettledExpression {
            plan: settled.plan.into_owned()?,
            statistics: settled.statistics,
            scopes: settled.scopes,
        })
    }

    pub(super) fn settle_arena(
        &mut self,
        plan: OwnedLogicalPlan,
        environment: &PlannerRuleEnvironment,
    ) -> Result<SettledExpression> {
        enum Task {
            Enter(PlanIndex, CteEnvironment),
            Consumer(PlanIndex, usize, CteEnvironment),
            Finish(PlanIndex, CteEnvironment, usize),
        }
        let mut input = LogicalPlanArena::default();
        let root = input.import_checked(plan, || environment.session.cancellation.check())?;
        let demands = demand::derive(&input, root, environment)?;
        let mut result = LogicalPlanArena::default();
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
            match task {
                Task::Enter(index, ctes) => {
                    let node = input.get(index)?;
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
                    let mut node = input.get(index)?.clone();
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
                        })?;
                        let statistics: SharedColumnStatistics = Arc::new(
                            reference
                                .bindings
                                .iter()
                                .copied()
                                .zip(reference.column_statistics())
                                .collect(),
                        );
                        scopes.insert(node.id, statistics.clone());
                        let aliases = reference
                            .bindings
                            .iter()
                            .map(|binding| (*binding, *binding))
                            .collect();
                        completed.push((result.append(node)?, facts, statistics, aliases));
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
                    )?;
                    let mut remapped = BTreeMap::new();
                    for recipe_index in self.recipes.post_order(local.recipe)? {
                        let recipe = self.recipes.get(recipe_index)?.clone();
                        if let LogicalOperator::BoundReference(reference) = &recipe.operator {
                            let child =
                                children
                                    .get(reference.reference_id as usize)
                                    .ok_or_else(|| {
                                        paro_error::internal(
                                            "settlement recipe changed its input contract",
                                        )
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
                        let mapped = result.append(LogicalPlanNode {
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
        Ok(SettledExpression {
            plan: paro_planner::plan::LogicalPlan::new(result, root)?,
            statistics,
            scopes,
        })
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
                        vec![Expression::Constant(ConstantExpression::new(
                            Value::Integer(row as i32),
                            LogicalType::Integer,
                        ))]
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
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                ))],
            )),
        )
    }

    #[test]
    fn a_local_boundary_caps_expected_ndv_without_inventing_a_hard_bound() {
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
            })
            .unwrap();
        assert_eq!(cache.facts[facts].columns[0].get_distinct_count(), 100);
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
            Some(100)
        );
        assert_eq!(
            reference.facts.column_domains[0].guaranteed_distinct_upper,
            None
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
        let second = cache
            .settle(
                project(&env.bind_context, values(&env.bind_context, 3)),
                &env,
            )
            .unwrap();
        assert_eq!(cache.misses, misses);
        assert_eq!(cache.hits, 2);
        assert_ne!(first.plan.id, second.plan.id);
        assert_eq!(first.plan.stats, second.plan.stats);
        assert_eq!(second.plan.stats.estimated_cardinality.unwrap().expected, 3);
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
                vec![Expression::Constant(ConstantExpression::new(
                    Value::Boolean(true),
                    LogicalType::Boolean,
                ))],
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
            LogicalOperator::Get(Get::new_without_table(
                0,
                vec!["a".into(), "b".into(), "c".into()],
                vec![LogicalType::Integer; 3],
            )),
        );
        let mut filter = Filter::new(
            scan,
            vec![Expression::Comparison(ComparisonExpression::new(
                ComparisonType::GreaterThan,
                Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 0),
                    LogicalType::Integer,
                )),
                Expression::Constant(ConstantExpression::new(
                    Value::Integer(0),
                    LogicalType::Integer,
                )),
            ))],
        );
        filter.projection_map = ProjectionMap::new(vec![0]);
        let plan = OwnedLogicalPlan::new(
            &env.bind_context,
            LogicalOperator::Projection(Projection::new(
                1,
                OwnedLogicalPlan::new(&env.bind_context, LogicalOperator::Filter(filter)),
                vec![Expression::ColumnRef(ColumnRefExpression::new(
                    ColumnBinding::new(0, 2),
                    LogicalType::Integer,
                ))],
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
        assert_eq!(statistics[0].get_distinct_count(), 30);
        assert_eq!(statistics[0].guaranteed_distinct_upper(), Some(90));
        assert!(!statistics[0].has_distinct_stats());
        assert_eq!(reference.facts.maximum_cardinality, Some(200));
    }
}
