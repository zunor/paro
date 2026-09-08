// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Separate output and execution demand on immutable arena edges. Binding
//! compaction is an explicit edge substitution, never a pointer into a parent.

use super::*;
use paro_planner::expression::ExpressionIterator;
use paro_planner::operator::ProjectionMap;

pub(super) type BindingMap = BTreeMap<ColumnBinding, ColumnBinding>;
pub(super) type ScanBindings = HashMap<
    (
        usize,
        Vec<paro_planner::operator::get::GetColumnSource>,
        Vec<LogicalType>,
    ),
    usize,
>;

pub(super) struct Demands {
    pub(super) layouts: BTreeMap<PlanIndex, LogicalOutputLayout>,
    pub(super) carriers: BTreeMap<PlanIndex, LogicalOutputLayout>,
    pub(super) outputs: BTreeMap<PlanIndex, BTreeSet<ColumnBinding>>,
}

pub(super) fn derive(
    arena: &LogicalPlanArena,
    root: PlanIndex,
    environment: &PlannerRuleEnvironment,
) -> Result<Option<Demands>> {
    let mut layouts = BTreeMap::<PlanIndex, LogicalOutputLayout>::new();
    let mut carriers = BTreeMap::<PlanIndex, LogicalOutputLayout>::new();
    let Some(post_order) =
        arena.post_order_controlled(root, || environment.control.checkpoint())?
    else {
        return Ok(None);
    };
    for index in post_order {
        environment.session.cancellation.check()?;
        if !environment.control.checkpoint()? {
            return Ok(None);
        }
        let node = arena.get(index)?;
        let mut children = Vec::new();
        node.operator
            .visit_child_links(&mut |child| children.push(layouts[child].clone()));
        layouts.insert(index, node.operator.output_layout_from_children(&children));
        let mut carrier_children = Vec::new();
        node.operator
            .visit_child_links(&mut |child| carrier_children.push(carriers[child].clone()));
        let mut operator = node.operator.clone();
        match &mut operator {
            LogicalOperator::Filter(filter) => filter.projection_map = ProjectionMap::all(),
            LogicalOperator::Order(order) => order.projection_map = ProjectionMap::all(),
            LogicalOperator::TopN(topn) => topn.projection_map = ProjectionMap::all(),
            LogicalOperator::Join(Join::Comparison(join)) => {
                join.left_projection_map = ProjectionMap::all();
                join.right_projection_map = ProjectionMap::all();
            }
            LogicalOperator::Join(Join::Any(join)) => {
                join.left_projection_map = ProjectionMap::all();
                join.right_projection_map = ProjectionMap::all();
            }
            _ => {}
        }
        carriers.insert(
            index,
            operator.output_layout_from_children(&carrier_children),
        );
    }
    let mut outputs = BTreeMap::from([(
        root,
        layouts[&root]
            .bindings()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>(),
    )]);
    let mut pending = vec![root];
    while let Some(index) = pending.pop() {
        environment.session.cancellation.check()?;
        if !environment.control.checkpoint()? {
            return Ok(None);
        }
        let node = arena.get(index)?;
        let wanted = outputs[&index].clone();
        let mut execution = wanted.clone();
        let mut positional = false;
        let mut operator = node.operator.clone();
        paro_planner::visitor::enumerate_expressions(&mut operator, |expression| {
            crate::expression::traversal::visit_expression(expression, &mut |expression| {
                if let Expression::ColumnRef(column) = expression {
                    if column.depth == 0 {
                        execution.insert(column.binding);
                    }
                }
                positional |= matches!(expression, Expression::Reference(_));
            });
        });
        let mut children = Vec::new();
        node.operator
            .visit_child_links(&mut |child| children.push(*child));
        for (ordinal, child) in children.into_iter().enumerate() {
            let all = positional
                || match &node.operator {
                    // These operators observe the complete row/positional schema,
                    // even if their parent returns only a subset.
                    LogicalOperator::SetOperation(_)
                    | LogicalOperator::Distinct(_)
                    | LogicalOperator::RecursiveCTE(_)
                    | LogicalOperator::EmptyResult(_) => true,
                    LogicalOperator::MaterializedCTE(_) => ordinal == 0,
                    LogicalOperator::Projection(_)
                    | LogicalOperator::Aggregate(_)
                    | LogicalOperator::Filter(_)
                    | LogicalOperator::Order(_)
                    | LogicalOperator::TopN(_)
                    | LogicalOperator::Limit(_)
                    | LogicalOperator::RowFetch(_)
                    | LogicalOperator::Window(_)
                    | LogicalOperator::Join(_) => false,
                    _ => true,
                };
            let layout = if all {
                &layouts[&child]
            } else {
                &carriers[&child]
            };
            let need = layout
                .bindings()
                .iter()
                .copied()
                .filter(|binding| all || execution.contains(binding))
                .collect::<BTreeSet<_>>();
            let first = !outputs.contains_key(&child);
            let prior = outputs.entry(child).or_default();
            let len = prior.len();
            prior.extend(need);
            if first || prior.len() != len {
                pending.push(child);
            }
        }
    }
    Ok(Some(Demands {
        layouts,
        carriers,
        outputs,
    }))
}

fn remap_expression(expression: &mut Expression, bindings: &BindingMap) {
    if let Expression::ColumnRef(column) = expression {
        if column.depth == 0 {
            if let Some(binding) = bindings.get(&column.binding) {
                column.binding = *binding;
            }
        }
    }
    ExpressionIterator::enumerate_children_mut(expression, |child| {
        remap_expression(child, bindings)
    });
}

fn project(
    projection: &mut ProjectionMap,
    before: &LogicalOutputLayout,
    after: &LogicalOutputLayout,
    bindings: &BindingMap,
    wanted: &BTreeSet<ColumnBinding>,
) -> Result<()> {
    let original = match projection.as_columns() {
        Some(columns) => columns.to_vec(),
        None => (0..before.len()).collect(),
    };
    let mut columns = Vec::new();
    let mut selected = BTreeSet::new();
    for ordinal in original {
        let binding = before
            .bindings()
            .get(ordinal)
            .ok_or_else(|| paro_error::internal("demand projection is out of bounds"))?;
        if !wanted.contains(binding) {
            continue;
        }
        selected.insert(*binding);
        let remapped = bindings
            .get(binding)
            .ok_or_else(|| paro_error::internal("output demand lost an input binding"))?;
        columns.push(
            after
                .bindings()
                .iter()
                .position(|candidate| candidate == remapped)
                .ok_or_else(|| {
                    paro_error::internal("execution demand removed an output carrier")
                })?,
        );
    }
    // Rewrites can introduce a parent-local use absent from the old map.
    // Widen only from an already-bound input's proven carrier interface.
    for binding in wanted.difference(&selected) {
        if let Some(remapped) = bindings.get(binding) {
            if let Some(ordinal) = after
                .bindings()
                .iter()
                .position(|candidate| candidate == remapped)
            {
                if !columns.contains(&ordinal) {
                    columns.push(ordinal);
                }
            }
        }
    }
    *projection = ProjectionMap::new(columns);
    Ok(())
}

pub(super) struct Inputs<'a> {
    pub(super) old_carriers: &'a LogicalOutputLayout,
    pub(super) before: &'a [LogicalOutputLayout],
    pub(super) after: &'a [LogicalOutputLayout],
    pub(super) children: &'a [BindingMap],
}

pub(super) fn apply(
    mut shell: LogicalPlanNode<()>,
    inputs: Inputs<'_>,
    wanted: &BTreeSet<ColumnBinding>,
    scan_bindings: &mut ScanBindings,
    bind: &BindContext,
) -> Result<(LogicalPlanNode<()>, BindingMap)> {
    let Inputs {
        old_carriers,
        before,
        after,
        children,
    } = inputs;
    let mut bindings = children
        .iter()
        .flat_map(|map| map.iter().map(|(a, b)| (*a, *b)))
        .collect::<BindingMap>();
    if let LogicalOperator::MaterializedCTE(cte) = &mut shell.operator {
        for column in &mut cte.output_columns {
            if let Some(binding) = bindings.get(&column.binding) {
                column.binding = *binding;
            }
        }
    }
    let get_output = matches!(shell.operator, LogicalOperator::Get(_));
    // Get's output identities are positional today. Compact its aligned
    // arrays atomically, then explicitly rebind every parent-local use.
    if let LogicalOperator::Get(get) = &mut shell.operator {
        let mut retained = wanted.clone();
        for expression in &get.runtime_filter_expressions {
            crate::expression::traversal::visit_expression(expression, &mut |expression| {
                if let Expression::ColumnRef(column) = expression {
                    if column.depth == 0 {
                        retained.insert(column.binding);
                    }
                }
            });
        }
        let positions = (0..get.returned_types.len())
            .filter(|ordinal| retained.contains(&ColumnBinding::new(get.table_index, *ordinal)))
            .collect::<Vec<_>>();
        let original_table = get.table_index;
        if positions.len() != get.returned_types.len() {
            let key = (
                original_table,
                positions
                    .iter()
                    .map(|ordinal| get.column_sources[*ordinal])
                    .collect(),
                positions
                    .iter()
                    .map(|ordinal| get.returned_types[*ordinal].clone())
                    .collect(),
            );
            get.table_index = *scan_bindings
                .entry(key)
                .or_insert_with(|| bind.generate_table_index());
        }
        for (new, old) in positions.iter().enumerate() {
            bindings.insert(
                ColumnBinding::new(original_table, *old),
                ColumnBinding::new(get.table_index, new),
            );
        }
        get.column_sources = positions
            .iter()
            .map(|index| get.column_sources[*index])
            .collect();
        get.column_types = positions
            .iter()
            .map(|index| get.column_types[*index].clone())
            .collect();
        get.returned_types = positions
            .iter()
            .map(|index| get.returned_types[*index].clone())
            .collect();
        get.names = positions
            .iter()
            .filter_map(|index| get.names.get(*index).cloned())
            .collect();
    }
    paro_planner::visitor::enumerate_expressions(&mut shell.operator, |expression| {
        remap_expression(expression, &bindings)
    });
    match &mut shell.operator {
        LogicalOperator::Filter(filter) => project(
            &mut filter.projection_map,
            &before[0],
            &after[0],
            &bindings,
            wanted,
        )?,
        LogicalOperator::Order(order) => project(
            &mut order.projection_map,
            &before[0],
            &after[0],
            &bindings,
            wanted,
        )?,
        LogicalOperator::TopN(topn) => project(
            &mut topn.projection_map,
            &before[0],
            &after[0],
            &bindings,
            wanted,
        )?,
        LogicalOperator::Join(Join::Comparison(join)) => {
            project(
                &mut join.left_projection_map,
                &before[0],
                &after[0],
                &bindings,
                wanted,
            )?;
            project(
                &mut join.right_projection_map,
                &before[1],
                &after[1],
                &bindings,
                wanted,
            )?;
        }
        LogicalOperator::Join(Join::Any(join)) => {
            project(
                &mut join.left_projection_map,
                &before[0],
                &after[0],
                &bindings,
                wanted,
            )?;
            project(
                &mut join.right_projection_map,
                &before[1],
                &after[1],
                &bindings,
                wanted,
            )?;
        }
        _ => {}
    }
    let output = shell.operator.output_layout_from_children(after);
    let map = old_carriers
        .bindings()
        .iter()
        .filter_map(|old| {
            if !bindings.contains_key(old)
                && (get_output || before.iter().any(|input| input.bindings().contains(old)))
            {
                return None;
            }
            let new = bindings.get(old).unwrap_or(old);
            output.bindings().contains(new).then_some((*old, *new))
        })
        .collect();
    shell.stats.invalidate_structural_facts();
    Ok((shell, map))
}
