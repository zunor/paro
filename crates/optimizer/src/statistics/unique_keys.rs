// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Shared declared-key proofs with explicit SQL NULL semantics.
//!
//! Catalog `UNIQUE` keys are enforced relational guarantees, whether or not a
//! physical index backs the constraint. Callers may rely on the declaration,
//! including SQL's allowance for multiple NULL tuples, but must make their
//! NULL equality semantics explicit in every proof.

use std::collections::{HashMap, HashSet};

use paro_catalog::entry::ConstraintType;
use paro_common::error::Result;
use paro_planner::expression::{ColumnRefExpression, Expression};
use paro_planner::operator::{
    ColumnBinding, Get, Join, JoinComparisonType, JoinCondition, JoinType, LogicalOperator,
};
use paro_planner::plan::{
    OwnedLogicalPlan, UniqueKey, UniqueKeyColumn, UniqueKeyNullSemantics, UniqueKeyProvenance,
};

/// Evidence that every candidate key binding is evaluated by an ordinary
/// equality predicate and therefore rejects NULL before uniqueness is used.
#[derive(Debug, Clone)]
pub(crate) struct NullRejectedKeyProof {
    keys: Box<[NullRejectedRightKey]>,
}

#[derive(Debug, Clone)]
struct NullRejectedRightKey {
    left: Expression,
    right: ColumnRefExpression,
}

impl NullRejectedKeyProof {
    pub(crate) fn from_equal_right_keys(conditions: &[JoinCondition]) -> Option<Self> {
        if conditions.is_empty() {
            return None;
        }
        let keys = conditions
            .iter()
            .cloned()
            .map(|condition| {
                if condition.comparison != JoinComparisonType::Equal {
                    return None;
                }
                let Expression::ColumnRef(right) = condition.right else {
                    return None;
                };
                if right.depth != 0 {
                    return None;
                }
                Some(NullRejectedRightKey {
                    left: condition.left,
                    right: right.into_inner(),
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { keys: keys.into() })
    }

    pub(crate) fn bindings(&self) -> impl Iterator<Item = ColumnBinding> + '_ {
        self.keys.iter().map(|key| key.right.binding)
    }

    /// Reconstruct the exact ordinary-equality conditions encoded by the
    /// witness. Equality is a type invariant rather than mutable payload.
    #[cfg(test)]
    pub(crate) fn conditions(&self) -> impl Iterator<Item = JoinCondition> + '_ {
        self.keys.iter().map(|key| {
            JoinCondition::new(
                key.left.clone(),
                Expression::ColumnRef(key.right.clone().into()),
                JoinComparisonType::Equal,
            )
        })
    }

    pub(crate) fn right_keys(&self) -> impl Iterator<Item = (&Expression, &ColumnRefExpression)> {
        self.keys.iter().map(|key| (&key.left, &key.right))
    }
}

pub(crate) struct DeclaredUniqueKey {
    pub(crate) bindings: Vec<ColumnBinding>,
    primary_key: bool,
}

impl DeclaredUniqueKey {
    pub(crate) fn is_unique_with_nulls_rejected(&self, proof: &NullRejectedKeyProof) -> bool {
        self.bindings
            .iter()
            .all(|binding| proof.bindings().any(|candidate| candidate == *binding))
    }

    /// Whether the declared key remains unique when NULL tuples compare equal,
    /// as they do in GROUP BY. Key coverage is deliberately a separate proof
    /// obligation so all callers use the same division of responsibility.
    pub(crate) fn is_unique_with_nulls_equal(
        &self,
        mut has_no_null: impl FnMut(ColumnBinding) -> bool,
    ) -> bool {
        self.primary_key || self.bindings.iter().copied().all(&mut has_no_null)
    }
}

pub(crate) fn declared_unique_keys(get: &Get) -> Vec<DeclaredUniqueKey> {
    let Some(table) = &get.table else {
        return Vec::new();
    };
    let mut column_indices = HashMap::with_capacity(get.column_sources.len());
    for column_index in 0..get.column_sources.len() {
        if let Some(column_id) = get.stored_column(column_index) {
            column_indices.entry(column_id).or_insert(column_index);
        }
    }
    table
        .constraints()
        .iter()
        .filter(|constraint| {
            matches!(
                constraint.constraint_type,
                ConstraintType::Unique | ConstraintType::PrimaryKey
            ) && !constraint.columns.is_empty()
        })
        .filter_map(|constraint| {
            let bindings = constraint
                .columns
                .iter()
                .map(|column_id| {
                    column_indices
                        .get(column_id)
                        .copied()
                        .map(|column_index| ColumnBinding::new(get.table_index, column_index))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(DeclaredUniqueKey {
                bindings,
                primary_key: constraint.constraint_type == ConstraintType::PrimaryKey,
            })
        })
        .collect()
}

/// Return cached keys as stable output bindings for statistics consumers.
pub(crate) fn proven_unique_keys(plan: &OwnedLogicalPlan) -> Vec<Vec<ColumnBinding>> {
    let layout = plan.output_layout();
    plan.stats
        .unique_keys
        .iter()
        .filter(|key| key_matches_layout(key, &layout))
        .map(|key| key.columns.iter().map(|column| column.binding).collect())
        .collect()
}

/// Prove that the supplied expressions cover one key of the current relation.
/// Callers remain responsible for proving ordinary-equality NULL rejection.
pub(crate) fn expressions_cover_unique_key(
    plan: &OwnedLogicalPlan,
    expressions: &[&Expression],
) -> bool {
    expressions_cover_key(plan, expressions, None)
}

/// Stronger proof used only by execution strategies that diagnose a duplicate
/// as a violated storage invariant rather than a planner-quality miss.
pub(crate) fn expressions_cover_catalog_unique_key(
    plan: &OwnedLogicalPlan,
    expressions: &[&Expression],
) -> bool {
    expressions_cover_key(
        plan,
        expressions,
        Some(UniqueKeyProvenance::CatalogEnforced),
    )
}

fn expressions_cover_key(
    plan: &OwnedLogicalPlan,
    expressions: &[&Expression],
    required_provenance: Option<UniqueKeyProvenance>,
) -> bool {
    let layout = plan.output_layout();
    !expressions.is_empty()
        && plan.stats.unique_keys.iter().any(|key| {
            required_provenance.is_none_or(|required| key.provenance == required)
                && !key.columns.is_empty()
                && key_matches_layout(key, &layout)
                && key.columns.iter().all(|column| {
                    expressions.iter().any(|expression| match expression {
                        Expression::Reference(reference) => reference.index == column.output_index,
                        Expression::ColumnRef(candidate) if candidate.depth == 0 => {
                            candidate.binding == column.binding
                        }
                        _ => false,
                    })
                })
        })
}

/// Cached witnesses are usable only while both halves of every positional
/// identity still describe the current output slot. Treat a mismatch as an
/// optimization miss: a cache is never allowed to manufacture a semantic
/// uniqueness proof.
fn key_matches_layout(
    key: &UniqueKey,
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> bool {
    key.columns
        .iter()
        .all(|column| layout.bindings().get(column.output_index).copied() == Some(column.binding))
}

/// Derive and cache keys for one node whose children have already completed
/// the statistics post-order fold.
pub(crate) fn derive_local_unique_keys(
    operator: &LogicalOperator,
    layout: &paro_planner::operator::LogicalOutputLayout,
    child_layouts: &[paro_planner::operator::LogicalOutputLayout],
) -> Vec<UniqueKey> {
    let children = operator.children();
    let keys = children
        .iter()
        .map(|child| child.stats.unique_keys.as_slice())
        .collect::<Vec<_>>();
    derive_unique_keys_from_facts(
        operator,
        layout,
        &child_layouts.iter().collect::<Vec<_>>(),
        &keys,
    )
}

/// One operator algebra shared by plan statistics and Memo-native facts.
/// Inputs are schemas and proof sets, never representative child trees.
pub(crate) fn derive_unique_keys_from_facts<Child>(
    operator: &LogicalOperator<Child>,
    layout: &paro_planner::operator::LogicalOutputLayout,
    child_layouts: &[&paro_planner::operator::LogicalOutputLayout],
    children: &[&[UniqueKey]],
) -> Vec<UniqueKey> {
    let mut keys = match operator {
        LogicalOperator::BoundReference(reference) => reference
            .facts
            .unique_keys
            .iter()
            .chain(&reference.facts.grouping_unique_keys)
            .cloned()
            .collect(),
        LogicalOperator::Get(get) => declared_keys_in_layout(get, layout),
        LogicalOperator::SearchScan(search) => {
            declared_keys_through_projection(&search.get, &search.projections, layout)
        }
        LogicalOperator::FullTextFilterScan(search) => declared_keys_in_layout(&search.get, layout),
        LogicalOperator::Filter(filter) => project_unique_keys(
            child_keys(children, 0),
            &filter
                .projection_map
                .to_indices(child_layout(child_layouts, 0).len()),
            layout,
        ),
        LogicalOperator::Order(order) => project_unique_keys(
            child_keys(children, 0),
            &order
                .projection_map
                .to_indices(child_layout(child_layouts, 0).len()),
            layout,
        ),
        LogicalOperator::TopN(topn) => project_unique_keys(
            child_keys(children, 0),
            &topn
                .projection_map
                .to_indices(child_layout(child_layouts, 0).len()),
            layout,
        ),
        LogicalOperator::Projection(projection) => {
            let sources = projection
                .expressions
                .iter()
                .map(|expression| {
                    expression_output_index(expression, child_layout(child_layouts, 0))
                })
                .collect::<Vec<_>>();
            remap_unique_keys(child_keys(children, 0), &sources, layout)
        }
        LogicalOperator::Limit(_)
        | LogicalOperator::Window(_)
        | LogicalOperator::EmptyResult(_)
        | LogicalOperator::RowFetch(_)
        | LogicalOperator::ExternalProject(_) => child_keys(children, 0).to_vec(),
        // One input carrier can expand to many edge rows. The input key alone
        // is therefore never a key of GraphExpand's output.
        LogicalOperator::GraphExpand(_) => Vec::new(),
        LogicalOperator::Distinct(_) => {
            let mut keys = child_keys(children, 0).to_vec();
            if !layout.is_empty() {
                keys.push(key_from_indices(
                    0..layout.len(),
                    layout,
                    UniqueKeyProvenance::Structural,
                ));
            }
            keys
        }
        LogicalOperator::Aggregate(aggregate) if aggregate.has_plain_grouping_domain() => {
            vec![key_from_indices(
                0..aggregate.groups.len(),
                layout,
                UniqueKeyProvenance::Structural,
            )]
        }
        LogicalOperator::Join(Join::Comparison(join))
            if join.duplicate_eliminated_columns.is_empty() && !join.delim_flipped =>
        {
            comparison_join_unique_keys(join, children, child_layouts, layout)
        }
        LogicalOperator::MaterializedCTE(_) => {
            remap_unique_keys_by_binding(child_keys(children, 1), layout, false)
        }
        _ => Vec::new(),
    };
    normalize_unique_keys(&mut keys);
    keys
}

/// Rebuild positional key witnesses after a pass changes output layouts.
///
/// Statistics gathering owns the normal derivation. Layout-rewriting passes
/// call this once at their public boundary so downstream consumers never see
/// bindings paired with stale output ordinals.
pub(crate) fn refresh_unique_keys(plan: OwnedLogicalPlan) -> Result<OwnedLogicalPlan> {
    plan.try_fold_post_order(|mut plan, child_layouts: Vec<_>| {
        let output_layout = plan.operator.output_layout_from_children(&child_layouts);
        plan.stats.unique_keys =
            derive_local_unique_keys(&plan.operator, &output_layout, &child_layouts);
        Ok((plan, output_layout))
    })
    .map(|(plan, _)| plan)
}

fn comparison_join_unique_keys<Child>(
    join: &paro_planner::operator::ComparisonJoin<Child>,
    children: &[&[UniqueKey]],
    child_layouts: &[&paro_planner::operator::LogicalOutputLayout],
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> Vec<UniqueKey> {
    let left_layout = child_layout(child_layouts, 0);
    let right_layout = child_layout(child_layouts, 1);
    let mut left_equalities = HashSet::new();
    let mut right_equalities = HashSet::new();
    for condition in &join.conditions {
        if condition.comparison != JoinComparisonType::Equal {
            continue;
        }
        let Some(left_index) = expression_output_index(&condition.left, left_layout) else {
            continue;
        };
        let Some(right_index) = expression_output_index(&condition.right, right_layout) else {
            continue;
        };
        left_equalities.insert(left_index);
        right_equalities.insert(right_index);
    }
    let left_join_key_unique = relation_key_is_covered(child_keys(children, 0), &left_equalities);
    let right_join_key_unique = relation_key_is_covered(child_keys(children, 1), &right_equalities);

    let mut keys = Vec::new();
    let preserve_left = match join.join_type {
        JoinType::Semi | JoinType::Anti | JoinType::Mark | JoinType::Single => true,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Outer => {
            right_join_key_unique
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::Invalid => false,
    };
    let preserve_right = match join.join_type {
        JoinType::RightSemi | JoinType::RightAnti => true,
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Outer | JoinType::Single => {
            left_join_key_unique
        }
        JoinType::Semi | JoinType::Anti | JoinType::Mark | JoinType::Invalid => false,
    };
    if preserve_left {
        let structural = matches!(
            join.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Outer
        );
        let mut preserved =
            remap_unique_keys_by_binding(child_keys(children, 0), layout, structural);
        if matches!(join.join_type, JoinType::Right | JoinType::Outer) {
            for key in &mut preserved {
                key.null_semantics = UniqueKeyNullSemantics::NullsDistinct;
            }
        }
        keys.extend(preserved);
    }
    if preserve_right {
        let structural = !matches!(join.join_type, JoinType::RightSemi | JoinType::RightAnti);
        let mut preserved =
            remap_unique_keys_by_binding(child_keys(children, 1), layout, structural);
        if matches!(
            join.join_type,
            JoinType::Left | JoinType::Outer | JoinType::Single
        ) {
            for key in &mut preserved {
                key.null_semantics = UniqueKeyNullSemantics::NullsDistinct;
            }
        }
        keys.extend(preserved);
    }
    keys
}

fn declared_key_null_semantics(get: &Get, key: &DeclaredUniqueKey) -> UniqueKeyNullSemantics {
    let null_safe = key.is_unique_with_nulls_equal(|binding| {
        get.stored_column(binding.column_index)
            .is_some_and(|column| {
                get.table
                    .as_ref()
                    .is_some_and(|table| table.column_is_declared_not_null(column))
            })
    });
    if null_safe {
        UniqueKeyNullSemantics::NullsEqual
    } else {
        UniqueKeyNullSemantics::NullsDistinct
    }
}

fn declared_keys_in_layout(
    get: &Get,
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> Vec<UniqueKey> {
    declared_unique_keys(get)
        .into_iter()
        .filter_map(|key| {
            let columns = key
                .bindings
                .iter()
                .map(|binding| {
                    layout
                        .bindings()
                        .iter()
                        .position(|candidate| candidate == binding)
                        .map(|output_index| UniqueKeyColumn {
                            output_index,
                            binding: *binding,
                        })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(UniqueKey::new(
                columns,
                UniqueKeyProvenance::CatalogEnforced,
                declared_key_null_semantics(get, &key),
            ))
        })
        .collect()
}

fn declared_keys_through_projection(
    get: &Get,
    expressions: &[Expression],
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> Vec<UniqueKey> {
    declared_unique_keys(get)
        .into_iter()
        .filter_map(|key| {
            let columns = key
                .bindings
                .iter()
                .map(|binding| {
                    expressions
                        .iter()
                        .position(|expression| match expression {
                            Expression::ColumnRef(column) if column.depth == 0 => {
                                column.binding == *binding
                            }
                            Expression::Reference(reference) => {
                                reference.index < get.returned_types.len()
                                    && ColumnBinding::new(get.table_index, reference.index)
                                        == *binding
                            }
                            _ => false,
                        })
                        .and_then(|output_index| {
                            layout.bindings().get(output_index).copied().map(|binding| {
                                UniqueKeyColumn {
                                    output_index,
                                    binding,
                                }
                            })
                        })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(UniqueKey::new(
                columns,
                UniqueKeyProvenance::CatalogEnforced,
                declared_key_null_semantics(get, &key),
            ))
        })
        .collect()
}

fn project_unique_keys(
    keys: &[UniqueKey],
    projected_child_indices: &[usize],
    output_layout: &paro_planner::operator::LogicalOutputLayout,
) -> Vec<UniqueKey> {
    keys.iter()
        .filter_map(|key| {
            let columns = key
                .columns
                .iter()
                .map(|column| {
                    projected_child_indices
                        .iter()
                        .position(|candidate| candidate == &column.output_index)
                        .and_then(|output_index| {
                            output_layout
                                .bindings()
                                .get(output_index)
                                .copied()
                                .map(|binding| UniqueKeyColumn {
                                    output_index,
                                    binding,
                                })
                        })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(UniqueKey::new(columns, key.provenance, key.null_semantics))
        })
        .collect()
}

fn remap_unique_keys(
    keys: &[UniqueKey],
    source_by_output: &[Option<usize>],
    output_layout: &paro_planner::operator::LogicalOutputLayout,
) -> Vec<UniqueKey> {
    keys.iter()
        .filter_map(|key| {
            let columns = key
                .columns
                .iter()
                .map(|column| {
                    source_by_output
                        .iter()
                        .position(|source| source == &Some(column.output_index))
                        .and_then(|output_index| {
                            output_layout
                                .bindings()
                                .get(output_index)
                                .copied()
                                .map(|binding| UniqueKeyColumn {
                                    output_index,
                                    binding,
                                })
                        })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(UniqueKey::new(columns, key.provenance, key.null_semantics))
        })
        .collect()
}

fn expression_output_index(
    expression: &Expression,
    layout: &paro_planner::operator::LogicalOutputLayout,
) -> Option<usize> {
    match expression {
        Expression::Reference(reference) => {
            (reference.index < layout.len()).then_some(reference.index)
        }
        Expression::ColumnRef(column) if column.depth == 0 => layout
            .bindings()
            .iter()
            .position(|binding| *binding == column.binding),
        _ => None,
    }
}

fn relation_key_is_covered(keys: &[UniqueKey], covered: &HashSet<usize>) -> bool {
    keys.iter().any(|key| {
        !key.columns.is_empty()
            && key
                .columns
                .iter()
                .all(|column| covered.contains(&column.output_index))
    })
}

fn normalize_unique_keys(keys: &mut Vec<UniqueKey>) {
    for key in keys.iter_mut() {
        let mut columns = key.columns.to_vec();
        columns.sort_unstable();
        columns.dedup_by_key(|column| column.output_index);
        key.columns = columns.into_boxed_slice();
    }
    keys.retain(|key| !key.columns.is_empty());
    keys.sort_by(|left, right| {
        left.columns
            .len()
            .cmp(&right.columns.len())
            .then_with(|| left.columns.cmp(&right.columns))
            .then_with(|| left.provenance.cmp(&right.provenance))
            .then_with(|| left.null_semantics.cmp(&right.null_semantics))
    });
    let mut retained = Vec::<UniqueKey>::new();
    for key in keys.drain(..) {
        if retained.iter().any(|candidate| {
            candidate.columns.iter().all(|column| {
                key.columns
                    .iter()
                    .any(|other| other.output_index == column.output_index)
            }) && candidate.provenance <= key.provenance
                && candidate.null_semantics <= key.null_semantics
        }) {
            continue;
        }
        retained.push(key);
    }
    *keys = retained;
}

fn child_layout<'a>(
    children: &[&'a paro_planner::operator::LogicalOutputLayout],
    index: usize,
) -> &'a paro_planner::operator::LogicalOutputLayout {
    children
        .get(index)
        .expect("unique-key derivation requires its logical child layout")
}

fn child_keys<'a>(children: &'a [&[UniqueKey]], index: usize) -> &'a [UniqueKey] {
    children
        .get(index)
        .copied()
        .expect("unique-key derivation requires its child facts")
}

fn key_from_indices(
    indices: impl IntoIterator<Item = usize>,
    layout: &paro_planner::operator::LogicalOutputLayout,
    provenance: UniqueKeyProvenance,
) -> UniqueKey {
    UniqueKey::new(
        indices.into_iter().map(|output_index| UniqueKeyColumn {
            output_index,
            binding: layout.bindings()[output_index],
        }),
        provenance,
        UniqueKeyNullSemantics::NullsEqual,
    )
}

fn remap_unique_keys_by_binding(
    keys: &[UniqueKey],
    output_layout: &paro_planner::operator::LogicalOutputLayout,
    structural: bool,
) -> Vec<UniqueKey> {
    // A ColumnBinding names one logical value even when a projection exposes
    // it more than once, so choosing the first matching output slot preserves
    // the key proof. Consumers still validate that slot against the binding
    // before turning the cached witness into a physical contract.
    keys.iter()
        .filter_map(|key| {
            let columns = key
                .columns
                .iter()
                .map(|column| {
                    output_layout
                        .bindings()
                        .iter()
                        .position(|binding| *binding == column.binding)
                        .map(|output_index| UniqueKeyColumn {
                            output_index,
                            binding: column.binding,
                        })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(UniqueKey::new(
                columns,
                if structural {
                    UniqueKeyProvenance::Structural
                } else {
                    key.provenance
                },
                key.null_semantics,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use paro_catalog::entry::{
        CatalogObjectId, ColumnDefinition, Constraint, CreateTableInfo, TableCatalogEntry,
    };
    use paro_common::types::LogicalType;
    use paro_planner::expression::{ColumnRefExpression, ReferenceExpression};
    use paro_planner::operator::ExpressionGet;

    use super::*;

    #[test]
    fn null_extension_key_proofs_match_an_independent_bag_oracle() {
        use paro_planner::operator::{ComparisonJoin, LogicalOutputLayout};
        let layouts = [1, 2].map(|table| {
            LogicalOutputLayout::new(
                vec![LogicalType::BigInt],
                vec![ColumnBinding::new(table, 0)],
            )
        });
        let output = LogicalOutputLayout::new(
            vec![LogicalType::BigInt; 2],
            vec![ColumnBinding::new(1, 0), ColumnBinding::new(2, 0)],
        );
        for left_nulls in 0..=2 {
            for right_nulls in 0..=2 {
                for left_mask in 0..8 {
                    for right_mask in 0..8 {
                        let bag = |mask: usize, nulls| {
                            (0..3)
                                .filter(|value| mask & (1 << value) != 0)
                                .map(Some)
                                .chain(std::iter::repeat_n(None, nulls))
                                .collect::<Vec<_>>()
                        };
                        let left = bag(left_mask, left_nulls);
                        let right = bag(right_mask, right_nulls);
                        let key = |index: usize, nulls| {
                            UniqueKey::new(
                                [UniqueKeyColumn {
                                    output_index: 0,
                                    binding: layouts[index].bindings()[0],
                                }],
                                UniqueKeyProvenance::Structural,
                                if nulls < 2 {
                                    UniqueKeyNullSemantics::NullsEqual
                                } else {
                                    UniqueKeyNullSemantics::NullsDistinct
                                },
                            )
                        };
                        let child_keys = [vec![key(0, left_nulls)], vec![key(1, right_nulls)]];
                        for kind in [
                            JoinType::Inner,
                            JoinType::Left,
                            JoinType::Right,
                            JoinType::Outer,
                        ] {
                            let join =
                                LogicalOperator::Join(Join::Comparison(ComparisonJoin::new(
                                    kind,
                                    OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                                    OwnedLogicalPlan::synthetic(LogicalOperator::DummyScan),
                                    vec![JoinCondition::equality(column(1, 0), column(2, 0))],
                                )));
                            let keys = derive_unique_keys_from_facts(
                                &join,
                                &output,
                                &layouts.iter().collect::<Vec<_>>(),
                                &[&child_keys[0], &child_keys[1]],
                            );
                            let mut rows = Vec::new();
                            for l in &left {
                                let mut matched = false;
                                for r in &right {
                                    if l.is_some() && l == r {
                                        rows.push([*l, *r]);
                                        matched = true;
                                    }
                                }
                                if !matched && matches!(kind, JoinType::Left | JoinType::Outer) {
                                    rows.push([*l, None]);
                                }
                            }
                            if matches!(kind, JoinType::Right | JoinType::Outer) {
                                for r in &right {
                                    if !r.is_some_and(|r| left.contains(&Some(r))) {
                                        rows.push([None, *r]);
                                    }
                                }
                            }
                            for key in keys {
                                let mut seen = HashSet::new();
                                for row in &rows {
                                    let tuple = key
                                        .columns
                                        .iter()
                                        .map(|column| row[column.output_index])
                                        .collect::<Vec<_>>();
                                    if key.null_semantics == UniqueKeyNullSemantics::NullsDistinct
                                        && tuple.contains(&None)
                                    {
                                        continue;
                                    }
                                    assert!(
                                        seen.insert(tuple),
                                        "{kind:?} {left:?} {right:?} falsely proved {key:?}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn catalog_grouping_uniqueness_requires_schema_not_a_null_free_observation() {
        use paro_storage::table::table_factory::TableFactory;
        use std::sync::Arc;
        for (primary, not_null, expected) in [
            (false, false, UniqueKeyNullSemantics::NullsDistinct),
            (false, true, UniqueKeyNullSemantics::NullsEqual),
            (true, false, UniqueKeyNullSemantics::NullsEqual),
        ] {
            let mut definition = ColumnDefinition::new("key".into(), LogicalType::BigInt);
            definition.not_null = not_null;
            let table = Arc::new(
                TableCatalogEntry::from_info(
                    CreateTableInfo::new(
                        "paro".into(),
                        "public".into(),
                        "keys".into(),
                        vec![definition],
                    )
                    .with_constraints(vec![if primary {
                        Constraint::primary_key(vec![0])
                    } else {
                        Constraint::unique(vec![0])
                    }]),
                    Arc::new(
                        TableFactory::default()
                            .create_table(&[LogicalType::BigInt])
                            .unwrap(),
                    ),
                    CatalogObjectId::from_raw(71_001),
                    0,
                )
                .unwrap(),
            );
            let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(Get::new(
                3,
                vec!["key".into()],
                vec![LogicalType::BigInt],
                table,
            ))));
            let keys = derive_local_unique_keys(&plan.operator, &plan.output_layout(), &[]);
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0].null_semantics, expected);
        }
    }

    #[test]
    fn weaker_nullable_key_does_not_prune_a_grouping_key() {
        let columns = [UniqueKeyColumn {
            output_index: 0,
            binding: ColumnBinding::new(1, 0),
        }];
        let mut keys = vec![
            UniqueKey::new(
                columns,
                UniqueKeyProvenance::CatalogEnforced,
                UniqueKeyNullSemantics::NullsDistinct,
            ),
            UniqueKey::new(
                columns,
                UniqueKeyProvenance::Structural,
                UniqueKeyNullSemantics::NullsEqual,
            ),
        ];
        normalize_unique_keys(&mut keys);
        assert_eq!(
            keys.len(),
            2,
            "catalog origin and NULL equality are independent obligations"
        );
    }

    fn column(table_index: usize, column_index: usize) -> Expression {
        Expression::ColumnRef(
            ColumnRefExpression::new(
                ColumnBinding::new(table_index, column_index),
                LogicalType::BigInt,
            )
            .into(),
        )
    }

    #[test]
    fn null_rejection_proof_requires_ordinary_equality() {
        let equal = JoinCondition::new(column(1, 0), column(2, 0), JoinComparisonType::Equal);
        let proof = NullRejectedKeyProof::from_equal_right_keys(&[equal.clone()])
            .expect("ordinary equality proves NULL rejection");
        let condition = proof.conditions().next().expect("sole equality condition");
        assert_eq!(condition.comparison, JoinComparisonType::Equal);
        assert!(matches!(&condition.right,
            Expression::ColumnRef(column) if column.binding == ColumnBinding::new(2, 0)));

        let null_safe = JoinCondition::new(
            column(1, 0),
            column(2, 0),
            JoinComparisonType::NotDistinctFrom,
        );
        assert!(NullRejectedKeyProof::from_equal_right_keys(&[null_safe]).is_none());
    }

    #[test]
    fn nullable_unique_key_requires_its_typed_null_rejection_proof() {
        let key = DeclaredUniqueKey {
            bindings: vec![ColumnBinding::new(2, 0), ColumnBinding::new(2, 1)],
            primary_key: false,
        };
        let conditions = [
            JoinCondition::new(column(1, 0), column(2, 0), JoinComparisonType::Equal),
            JoinCondition::new(column(1, 1), column(2, 1), JoinComparisonType::Equal),
        ];
        let complete = NullRejectedKeyProof::from_equal_right_keys(&conditions).unwrap();
        assert!(key.is_unique_with_nulls_rejected(&complete));

        let incomplete = NullRejectedKeyProof::from_equal_right_keys(&conditions[..1]).unwrap();
        assert!(!key.is_unique_with_nulls_rejected(&incomplete));
    }

    #[test]
    fn stale_positional_key_fails_closed_against_current_layout() {
        let mut plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::ExpressionGet(ExpressionGet::new(
                7,
                Vec::new(),
                vec!["a".to_string(), "b".to_string()],
                vec![LogicalType::BigInt, LogicalType::BigInt],
            )));
        plan.stats.unique_keys.push(UniqueKey::new(
            [UniqueKeyColumn {
                output_index: 0,
                binding: ColumnBinding::new(7, 1),
            }],
            UniqueKeyProvenance::CatalogEnforced,
            UniqueKeyNullSemantics::NullsDistinct,
        ));
        let reference =
            Expression::Reference(ReferenceExpression::new(0, LogicalType::BigInt).into());

        assert!(!expressions_cover_catalog_unique_key(&plan, &[&reference]));
        assert!(proven_unique_keys(&plan).is_empty());
    }
}
