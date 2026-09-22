// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Allocation-free structural preconditions for planner transformations.

use std::cmp::Reverse;

use super::*;

/// Bind only the operator skeleton consumed by dimension sharing.
///
/// The generic matcher enumerates the Cartesian product of every descendant
/// alternative. Dimension sharing instead consumes a fixed local pattern on
/// either side of a UNION and treats the input below each partial aggregate as
/// one opaque, semantically equivalent choice. Keeping that choice exact lets
/// the current semantic adapter operate without a representative-tree bridge,
/// while avoiding work exponential in unrelated descendant alternatives.
pub(super) fn dimension_sharing_pattern_bindings(
    root_group: GroupId,
    root_expression: LogicalExprId,
    memo: &Memo,
    state: &PlannerTransformState,
    budget: &SearchBudget,
    cancellation: Option<&paro_context::StatementCancellation>,
) -> Result<PatternBindingSet> {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct BranchSkeleton {
        projection: LogicalExprId,
        filter: Option<LogicalExprId>,
        outer_aggregate: LogicalExprId,
        join: LogicalExprId,
        dimension: LogicalExprId,
        partial_aggregate: LogicalExprId,
    }

    #[derive(Debug, Clone)]
    enum UnionSkeleton {
        Branch {
            group: GroupId,
            branch: BranchSkeleton,
        },
        SetOperation {
            group: GroupId,
            expression: LogicalExprId,
            left: Box<UnionSkeleton>,
            right: Box<UnionSkeleton>,
        },
    }

    impl UnionSkeleton {
        fn branches(&self, output: &mut Vec<BranchSkeleton>) {
            match self {
                Self::Branch { branch, .. } => output.push(*branch),
                Self::SetOperation { left, right, .. } => {
                    left.branches(output);
                    right.branches(output);
                }
            }
        }
    }

    struct LocalMatcher<'a> {
        memo: &'a Memo,
        state: &'a PlannerTransformState,
        limit: usize,
        work_units: usize,
        reads: BTreeMap<GroupId, PatternRead>,
        limited: bool,
        cancellation: Option<&'a paro_context::StatementCancellation>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum NativeOutputSlot {
        Group(usize),
        Aggregate(usize),
        Constant,
    }

    #[derive(Debug, Clone)]
    struct NativeBranch<'a> {
        projection: &'a paro_planner::operator::Projection<()>,
        filter: Option<&'a paro_planner::operator::Filter<()>>,
        outer: &'a paro_planner::operator::Aggregate<()>,
        join: &'a paro_planner::operator::join::ComparisonJoin<()>,
        dimension: &'a paro_planner::operator::Get,
        partial: &'a paro_planner::operator::Aggregate<()>,
        output_slots: Vec<NativeOutputSlot>,
    }

    impl LocalMatcher<'_> {
        fn admit_work(&mut self, units: usize) -> Result<bool> {
            if !self.memo.control().checkpoint()? {
                self.limited = true;
                return Ok(false);
            }
            if let Some(cancellation) = self.cancellation {
                cancellation.check()?;
            }
            if self.work_units.saturating_add(units) > self.limit {
                self.limited = true;
                return Ok(false);
            }
            self.work_units += units;
            Ok(true)
        }

        fn observe(&mut self, group: GroupId) -> Result<bool> {
            let group = self.memo.canonical_group(group);
            if let Some(read) = self.reads.get_mut(&group) {
                if read.logical_frontier_revision.is_none() {
                    *read = PatternRead::from_group(self.memo, group)?;
                }
                return Ok(true);
            }
            if !self.admit_work(1)? {
                return Ok(false);
            }
            self.reads
                .insert(group, PatternRead::from_group(self.memo, group)?);
            Ok(true)
        }

        fn observe_facts(&mut self, group: GroupId) -> Result<bool> {
            let group = self.memo.canonical_group(group);
            if self.reads.contains_key(&group) {
                return Ok(true);
            }
            if !self.admit_work(1)? {
                return Ok(false);
            }
            self.reads
                .insert(group, PatternRead::facts_from_group(self.memo, group)?);
            Ok(true)
        }

        fn logical(
            &self,
            expression: LogicalExprId,
        ) -> Result<&crate::cascades::memo::LogicalExpr> {
            self.memo.logical_expr(expression).ok_or_else(|| {
                paro_error::internal("local pattern matcher references an unknown expression")
            })
        }

        fn operator_type(&self, expression: LogicalExprId) -> Option<LogicalOperatorType> {
            let logical = self.memo.logical_expr(expression)?;
            self.state
                .metadata
                .get(&logical.payload)
                .map(|metadata| metadata.operator_type)
        }

        fn dimension_get(&self, expression: LogicalExprId) -> Option<&paro_planner::operator::Get> {
            let logical = self.memo.logical_expr(expression)?;
            let payload = self.state.payloads.logical.get(logical.payload.index())?;
            let LogicalOperator::Get(get) = &payload.semantic_template.operator else {
                return None;
            };
            Some(get)
        }

        /// Read only the immutable operator shells needed by dimension
        /// sharing.  The semantic template has no owned child tree; all
        /// descendants are represented by the exact Memo expression ids in
        /// the skeleton.  Keeping this check here avoids constructing an
        /// OwnedLogicalPlan for every frontier pair merely to call the
        /// legacy recognizer.
        fn semantic_operator(
            &self,
            expression: LogicalExprId,
        ) -> Option<&paro_planner::operator::LogicalOperator<()>> {
            let logical = self.memo.logical_expr(expression)?;
            self.state
                .payloads
                .logical
                .get(logical.payload.index())
                .map(|payload| &payload.semantic_template.operator)
        }

        fn native_branch(&self, skeleton: BranchSkeleton) -> Option<NativeBranch<'_>> {
            let projection = match self.semantic_operator(skeleton.projection)? {
                LogicalOperator::Projection(projection) => projection,
                _ => return None,
            };
            let filter = skeleton.filter.and_then(|expression| {
                match self.semantic_operator(expression)? {
                    LogicalOperator::Filter(filter) => Some(filter),
                    _ => None,
                }
            });
            if skeleton.filter.is_some() && filter.is_none() {
                return None;
            }
            let outer = match self.semantic_operator(skeleton.outer_aggregate)? {
                LogicalOperator::Aggregate(aggregate) => aggregate.as_ref(),
                _ => return None,
            };
            let join = match self.semantic_operator(skeleton.join)? {
                LogicalOperator::Join(Join::Comparison(join)) => join,
                _ => return None,
            };
            let dimension = match self.semantic_operator(skeleton.dimension)? {
                LogicalOperator::Get(dimension) => dimension,
                _ => return None,
            };
            let partial = match self.semantic_operator(skeleton.partial_aggregate)? {
                LogicalOperator::Aggregate(aggregate) => aggregate.as_ref(),
                _ => return None,
            };
            if outer.post_reduction.is_some()
                || outer.aggregates.is_empty()
                || !outer.has_plain_grouping_domain()
                || join.join_type != JoinType::Inner
                || join.conditions.is_empty()
                || join.mark_index.is_some()
                || !join.duplicate_eliminated_columns.is_empty()
                || join.delim_flipped
                || join.build_side_constraint
                    != paro_planner::operator::JoinBuildSideConstraint::Either
                || join
                    .conditions
                    .iter()
                    .any(|condition| condition.comparison != JoinComparisonType::Equal)
                || partial.post_reduction.is_some()
                || partial.aggregates.is_empty()
                || !partial.has_plain_grouping_domain()
                || dimension.table.is_none()
            {
                return None;
            }
            let dimension_bindings = (0..dimension.returned_types.len())
                .map(|ordinal| ColumnBinding::new(dimension.table_index, ordinal))
                .collect::<std::collections::HashSet<_>>();
            if !join.conditions.iter().all(|condition| {
                expression_reads_only(&condition.left, &dimension_bindings)
                    && matches!(
                        &condition.right,
                        Expression::ColumnRef(column)
                            if column.depth == 0
                                && column.binding.table_index == partial.group_index
                                && column.binding.column_index < partial.groups.len()
                    )
            }) || !merge_contract_matches_partial(outer, partial)
            {
                return None;
            }
            let output_slots = projection
                .expressions
                .iter()
                .map(|expression| output_slot(expression, outer))
                .collect::<Option<Vec<_>>>()?;
            Some(NativeBranch {
                projection,
                filter,
                outer,
                join,
                dimension,
                partial,
                output_slots,
            })
        }

        fn union_operator_is_valid(&self, expression: LogicalExprId) -> bool {
            let Some(LogicalOperator::SetOperation(setop)) = self.semantic_operator(expression)
            else {
                return false;
            };
            setop.setop_type == paro_planner::operator::SetOpType::Union
                && setop.setop_all
                && setop.column_count == setop.types.len()
        }

        fn branches_compatible(&self, left: &NativeBranch<'_>, right: &NativeBranch<'_>) -> bool {
            if left.projection.expressions.len() != right.projection.expressions.len()
                || left.outer.groups.len() != right.outer.groups.len()
                || left.outer.aggregates.len() != right.outer.aggregates.len()
                || left.partial.groups.len() != right.partial.groups.len()
                || left.partial.aggregates.len() != right.partial.aggregates.len()
                || left.join.conditions.len() != right.join.conditions.len()
                || left
                    .partial
                    .groups
                    .iter()
                    .map(Expression::return_type)
                    .ne(right.partial.groups.iter().map(Expression::return_type))
                || left
                    .partial
                    .aggregates
                    .iter()
                    .map(Expression::return_type)
                    .ne(right.partial.aggregates.iter().map(Expression::return_type))
                || left.output_slots != right.output_slots
            {
                return false;
            }
            if !crate::aggregate::dimension_sharing::equivalent_dimension_gets(
                left.dimension,
                right.dimension,
            ) {
                return false;
            }
            let mut bindings = HashMap::new();
            extend_positional_bindings(
                &mut bindings,
                right.dimension.table_index,
                left.dimension.table_index,
                left.dimension.returned_types.len(),
            );
            extend_positional_bindings(
                &mut bindings,
                right.partial.group_index,
                left.partial.group_index,
                left.partial.groups.len(),
            );
            extend_positional_bindings(
                &mut bindings,
                right.partial.aggregate_index,
                left.partial.aggregate_index,
                left.partial.aggregates.len(),
            );
            extend_positional_bindings(
                &mut bindings,
                right.outer.group_index,
                left.outer.group_index,
                left.outer.groups.len(),
            );
            extend_positional_bindings(
                &mut bindings,
                right.outer.aggregate_index,
                left.outer.aggregate_index,
                left.outer.aggregates.len(),
            );
            let filters_match = match (left.filter, right.filter) {
                (None, None) => true,
                (Some(left), Some(right)) => {
                    let right = right
                        .expressions
                        .iter()
                        .map(|expression| remap_expression(expression, &bindings))
                        .collect::<Option<Vec<_>>>();
                    right.is_some_and(|right| expression_multisets_equal(&left.expressions, &right))
                }
                _ => false,
            };
            filters_match
                && left
                    .outer
                    .groups
                    .iter()
                    .zip(&right.outer.groups)
                    .all(|(left, right)| {
                        remap_expression(right, &bindings).is_some_and(|right| left.equals(&right))
                    })
                && left
                    .outer
                    .aggregates
                    .iter()
                    .zip(&right.outer.aggregates)
                    .all(|(left, right)| {
                        remap_expression(right, &bindings).is_some_and(|right| left.equals(&right))
                    })
                && left
                    .join
                    .conditions
                    .iter()
                    .zip(&right.join.conditions)
                    .all(|(left, right)| {
                        left.comparison == right.comparison
                            && remap_expression(&right.left, &bindings)
                                .is_some_and(|right| left.left.equals(&right))
                            && remap_expression(&right.right, &bindings)
                                .is_some_and(|right| left.right.equals(&right))
                    })
                && left
                    .projection
                    .expressions
                    .iter()
                    .zip(&right.projection.expressions)
                    .zip(&left.output_slots)
                    .all(|((left, right), slot)| match slot {
                        NativeOutputSlot::Constant => left.return_type() == right.return_type(),
                        _ => remap_expression(right, &bindings)
                            .is_some_and(|right| left.equals(&right)),
                    })
        }

        fn union_contract_matches(&self, skeleton: &UnionSkeleton) -> bool {
            let mut branches = Vec::new();
            skeleton.branches(&mut branches);
            if let UnionSkeleton::SetOperation { expression, .. } = skeleton {
                if !self.union_operator_is_valid(*expression) {
                    return false;
                }
            }
            let Some(first) = branches.first().copied() else {
                return false;
            };
            let Some(first) = self.native_branch(first) else {
                return false;
            };
            let output_types = first.projection.returned_types.as_slice();
            let root_types_match = match skeleton {
                UnionSkeleton::SetOperation { expression, .. } => {
                    self.semantic_operator(*expression).is_some_and(|operator| {
                        matches!(operator, LogicalOperator::SetOperation(setop)
                            if setop.types.as_slice() == output_types
                                && setop.column_count == output_types.len())
                    })
                }
                UnionSkeleton::Branch { .. } => false,
            };
            if output_types.len() != first.projection.expressions.len() || !root_types_match {
                return false;
            }
            for branch in branches.iter().skip(1).copied() {
                let Some(branch) = self.native_branch(branch) else {
                    return false;
                };
                if branch.projection.returned_types != output_types
                    || !self.branches_compatible(&first, &branch)
                {
                    return false;
                }
            }
            true
        }

        fn dimensions_can_share(&self, left: BranchSkeleton, right: BranchSkeleton) -> bool {
            self.dimension_get(left.dimension)
                .zip(self.dimension_get(right.dimension))
                .is_some_and(|(left, right)| {
                    crate::aggregate::dimension_sharing::equivalent_dimension_gets(left, right)
                })
        }

        fn union_dimensions_can_share(&self, skeleton: &UnionSkeleton) -> bool {
            let mut branches = Vec::new();
            skeleton.branches(&mut branches);
            let Some(first) = branches.first().copied() else {
                return false;
            };
            branches
                .iter()
                .copied()
                .skip(1)
                .all(|branch| self.dimensions_can_share(first, branch))
        }

        fn operator_fingerprint(&self, expression: LogicalExprId) -> Fingerprint {
            self.memo
                .logical_expr(expression)
                .map(|logical| logical.key.operator)
                .unwrap_or_default()
        }

        fn expressions_of_type(
            &mut self,
            group: GroupId,
            operator_type: LogicalOperatorType,
        ) -> Result<Vec<LogicalExprId>> {
            let group = self.memo.canonical_group(group);
            if !self.observe(group)? {
                return Ok(Vec::new());
            }
            let group_ref = self.memo.group(group).ok_or_else(|| {
                paro_error::internal("local pattern matcher references an unknown group")
            })?;
            let mut expressions = group_ref
                .logical_exprs_of_operator(super::super::identity::operator_tag(operator_type))
                .iter()
                .copied()
                .chain(
                    group_ref
                        .logical_exprs_of_operator(
                            crate::cascades::memo::UNTYPED_LOGICAL_OPERATOR_TAG,
                        )
                        .iter()
                        .copied(),
                )
                .filter(|expression| self.operator_type(*expression) == Some(operator_type))
                .collect::<Vec<_>>();
            expressions.sort_by_key(|expression| self.operator_fingerprint(*expression));
            Ok(expressions)
        }

        fn branch_skeletons(&mut self, group: GroupId) -> Result<Vec<BranchSkeleton>> {
            let mut skeletons = Vec::new();
            for projection in self.expressions_of_type(group, LogicalOperatorType::Projection)? {
                let projection_logical = self.logical(projection)?;
                let [projection_child] = projection_logical.key.children.as_ref() else {
                    continue;
                };
                let mut aggregate_parents = vec![(None, *projection_child)];
                for filter in
                    self.expressions_of_type(*projection_child, LogicalOperatorType::Filter)?
                {
                    let filter_logical = self.logical(filter)?;
                    if let [filter_child] = filter_logical.key.children.as_ref() {
                        aggregate_parents.push((Some(filter), *filter_child));
                    }
                }
                for (filter, aggregate_group) in aggregate_parents {
                    for outer_aggregate in
                        self.expressions_of_type(aggregate_group, LogicalOperatorType::Aggregate)?
                    {
                        let outer_logical = self.logical(outer_aggregate)?;
                        let [join_group] = outer_logical.key.children.as_ref() else {
                            continue;
                        };
                        for join in self
                            .expressions_of_type(*join_group, LogicalOperatorType::ComparisonJoin)?
                        {
                            let (dimension_group, partial_group) = {
                                let join_logical = self.logical(join)?;
                                let [dimension_group, partial_group] =
                                    join_logical.key.children.as_ref()
                                else {
                                    continue;
                                };
                                (*dimension_group, *partial_group)
                            };
                            let dimensions = self
                                .expressions_of_type(dimension_group, LogicalOperatorType::Get)?;
                            let partials = self.expressions_of_type(
                                partial_group,
                                LogicalOperatorType::Aggregate,
                            )?;
                            for dimension in &dimensions {
                                for partial_aggregate in &partials {
                                    skeletons.push(BranchSkeleton {
                                        projection,
                                        filter,
                                        outer_aggregate,
                                        join,
                                        dimension: *dimension,
                                        partial_aggregate: *partial_aggregate,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            skeletons.sort_by_key(|skeleton| {
                (
                    Reverse(
                        self.dimension_get(skeleton.dimension)
                            .map_or(0, |get| get.returned_types.len()),
                    ),
                    self.operator_fingerprint(skeleton.projection),
                    skeleton
                        .filter
                        .map(|filter| self.operator_fingerprint(filter))
                        .unwrap_or_default(),
                    self.operator_fingerprint(skeleton.outer_aggregate),
                    self.operator_fingerprint(skeleton.join),
                    self.operator_fingerprint(skeleton.dimension),
                    self.operator_fingerprint(skeleton.partial_aggregate),
                )
            });
            skeletons.dedup();
            Ok(skeletons)
        }

        fn expression_operand(
            &mut self,
            group: GroupId,
            expression: LogicalExprId,
            children: Vec<(PatternOperand, Fingerprint)>,
        ) -> Result<Option<(PatternOperand, Fingerprint)>> {
            if !self.admit_work(1)? {
                return Ok(None);
            }
            let logical = self.logical(expression)?;
            if logical.key.children.len() != children.len() {
                return Err(paro_error::internal(
                    "local pattern binding child arity disagrees with expression",
                ));
            }
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_bytes(b"paro.pattern.binding.v1");
            fingerprint.write_fingerprint(logical.key.operator);
            for (_, child_fingerprint) in &children {
                fingerprint.write_fingerprint(*child_fingerprint);
            }
            Ok(Some((
                PatternOperand::Expression {
                    group: self.memo.canonical_group(group),
                    expression,
                    children: children
                        .into_iter()
                        .map(|(operand, _)| operand)
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                },
                fingerprint.finish(),
            )))
        }

        fn group_hole(&mut self, group: GroupId) -> Result<Option<(PatternOperand, Fingerprint)>> {
            let group = self.memo.canonical_group(group);
            // The rule deliberately does not inspect the fact subtree. Facts
            // still participate in the read cursor because staging and
            // costing consume the group's schema/cardinality contract.
            if !self.observe_facts(group)? || !self.admit_work(1)? {
                return Ok(None);
            }
            let group_ref = self.memo.group(group).ok_or_else(|| {
                paro_error::internal("local pattern group hole references an unknown group")
            })?;
            let mut fingerprint = StableFingerprintBuilder::default();
            fingerprint.write_bytes(b"paro.pattern.semantic-group-hole.v1");
            fingerprint.write_u64(group.0 as u64);
            fingerprint.write_fingerprint(group_ref.logical_fact_fingerprint());
            fingerprint.write_fingerprint(group_ref.statistics_snapshot_fingerprint());
            Ok(Some((PatternOperand::Group(group), fingerprint.finish())))
        }

        fn branch_operand(
            &mut self,
            group: GroupId,
            skeleton: BranchSkeleton,
        ) -> Result<Option<(PatternOperand, Fingerprint)>> {
            let projection_child = {
                let projection_logical = self.logical(skeleton.projection)?;
                let [projection_child] = projection_logical.key.children.as_ref() else {
                    return Ok(None);
                };
                *projection_child
            };
            let aggregate_group = if let Some(filter) = skeleton.filter {
                let filter_logical = self.logical(filter)?;
                let [aggregate_group] = filter_logical.key.children.as_ref() else {
                    return Ok(None);
                };
                *aggregate_group
            } else {
                projection_child
            };
            let join_group = {
                let outer_logical = self.logical(skeleton.outer_aggregate)?;
                let [join_group] = outer_logical.key.children.as_ref() else {
                    return Ok(None);
                };
                *join_group
            };
            let (dimension_group, partial_group) = {
                let join_logical = self.logical(skeleton.join)?;
                let [dimension_group, partial_group] = join_logical.key.children.as_ref() else {
                    return Ok(None);
                };
                (*dimension_group, *partial_group)
            };
            let partial_child_groups = self
                .logical(skeleton.partial_aggregate)?
                .key
                .children
                .clone();
            let mut partial_children = Vec::with_capacity(partial_child_groups.len());
            for child in partial_child_groups.iter().copied() {
                let Some(child) = self.group_hole(child)? else {
                    return Ok(None);
                };
                partial_children.push(child);
            }
            let Some(partial) = self.expression_operand(
                partial_group,
                skeleton.partial_aggregate,
                partial_children,
            )?
            else {
                return Ok(None);
            };
            let Some(dimension) =
                self.expression_operand(dimension_group, skeleton.dimension, Vec::new())?
            else {
                return Ok(None);
            };
            let Some(join) =
                self.expression_operand(join_group, skeleton.join, vec![dimension, partial])?
            else {
                return Ok(None);
            };
            let Some(outer) =
                self.expression_operand(aggregate_group, skeleton.outer_aggregate, vec![join])?
            else {
                return Ok(None);
            };
            let child = if let Some(filter) = skeleton.filter {
                let Some(filter) =
                    self.expression_operand(projection_child, filter, vec![outer])?
                else {
                    return Ok(None);
                };
                filter
            } else {
                outer
            };
            self.expression_operand(group, skeleton.projection, vec![child])
        }

        fn union_skeletons(&mut self, group: GroupId) -> Result<Vec<UnionSkeleton>> {
            let group = self.memo.canonical_group(group);
            let mut skeletons = self
                .branch_skeletons(group)?
                .into_iter()
                // The legacy recognizer rejects a malformed branch before
                // pairing it with any other arm. Keep that same early
                // rejection here; otherwise the native Cartesian product
                // can grow from every projection/aggregate shell in the
                // child frontier and only be discarded at the root.
                .filter(|branch| self.native_branch(*branch).is_some())
                .map(|branch| UnionSkeleton::Branch { group, branch })
                .collect::<Vec<_>>();
            for expression in self.expressions_of_type(group, LogicalOperatorType::LogicalUnion)? {
                if !self.union_operator_is_valid(expression) {
                    continue;
                }
                let (left_group, right_group) = {
                    let logical = self.logical(expression)?;
                    let [left_group, right_group] = logical.key.children.as_ref() else {
                        continue;
                    };
                    (*left_group, *right_group)
                };
                let left = self.union_skeletons(left_group)?;
                let right = self.union_skeletons(right_group)?;
                'pairs: for left in &left {
                    for right in &right {
                        if !self.admit_work(1)? {
                            break 'pairs;
                        }
                        skeletons.push(UnionSkeleton::SetOperation {
                            group,
                            expression,
                            left: Box::new(left.clone()),
                            right: Box::new(right.clone()),
                        });
                    }
                }
                if self.limited {
                    break;
                }
            }
            skeletons.sort_by_key(|skeleton| self.union_skeleton_fingerprint(skeleton));
            skeletons.dedup_by_key(|skeleton| self.union_skeleton_fingerprint(skeleton));
            Ok(skeletons)
        }

        fn union_skeleton_fingerprint(&self, skeleton: &UnionSkeleton) -> Fingerprint {
            let mut fingerprint = StableFingerprintBuilder::default();
            match skeleton {
                UnionSkeleton::Branch { group, branch } => {
                    fingerprint.write_bytes(b"paro.dimension-sharing.branch.v1");
                    fingerprint.write_u64(group.0 as u64);
                    fingerprint.write_fingerprint(self.operator_fingerprint(branch.projection));
                    fingerprint
                        .write_fingerprint(self.operator_fingerprint(branch.partial_aggregate));
                }
                UnionSkeleton::SetOperation {
                    group,
                    expression,
                    left,
                    right,
                } => {
                    fingerprint.write_bytes(b"paro.dimension-sharing.nary-union.v1");
                    fingerprint.write_u64(group.0 as u64);
                    fingerprint.write_fingerprint(self.operator_fingerprint(*expression));
                    fingerprint.write_fingerprint(self.union_skeleton_fingerprint(left));
                    fingerprint.write_fingerprint(self.union_skeleton_fingerprint(right));
                }
            }
            fingerprint.finish()
        }

        fn union_operand(
            &mut self,
            skeleton: &UnionSkeleton,
        ) -> Result<Option<(PatternOperand, Fingerprint)>> {
            match skeleton {
                UnionSkeleton::Branch { group, branch } => self.branch_operand(*group, *branch),
                UnionSkeleton::SetOperation {
                    group,
                    expression,
                    left,
                    right,
                } => {
                    let Some(left) = self.union_operand(left)? else {
                        return Ok(None);
                    };
                    let Some(right) = self.union_operand(right)? else {
                        return Ok(None);
                    };
                    self.expression_operand(*group, *expression, vec![left, right])
                }
            }
        }

        fn operand_nodes(operand: &PatternOperand) -> usize {
            match operand {
                PatternOperand::Group(_) => 1,
                PatternOperand::Expression { children, .. } => 1usize.saturating_add(
                    children
                        .iter()
                        .map(Self::operand_nodes)
                        .fold(0usize, usize::saturating_add),
                ),
            }
        }
    }

    fn expression_reads_only(
        expression: &Expression,
        allowed: &std::collections::HashSet<ColumnBinding>,
    ) -> bool {
        let mut valid = true;
        let mut read = false;
        crate::expression::traversal::visit_expression(expression, &mut |expression| {
            if let Expression::ColumnRef(column) = expression {
                read = true;
                valid &= column.depth == 0 && allowed.contains(&column.binding);
            }
        });
        valid && read
    }

    fn merge_contract_matches_partial(
        outer: &paro_planner::operator::Aggregate<()>,
        partial: &paro_planner::operator::Aggregate<()>,
    ) -> bool {
        outer.aggregates.iter().all(|expression| {
            let Expression::Aggregate(merge) = expression else {
                return false;
            };
            if merge.children.len() != 1 || merge.filter.is_some() || !merge.order_bys.is_empty() {
                return false;
            }
            let Expression::ColumnRef(column) = &merge.children[0] else {
                return false;
            };
            if column.depth != 0
                || column.binding.table_index != partial.aggregate_index
                || column.binding.column_index >= partial.aggregates.len()
            {
                return false;
            }
            let Expression::Aggregate(source) = &partial.aggregates[column.binding.column_index]
            else {
                return false;
            };
            source
                .function
                .partial_merge_function()
                .is_some_and(|expected| expected.execution_semantics_equal(&merge.function))
        })
    }

    fn output_slot(
        expression: &Expression,
        aggregate: &paro_planner::operator::Aggregate<()>,
    ) -> Option<NativeOutputSlot> {
        match expression {
            Expression::ColumnRef(column) if column.depth == 0 => {
                if column.binding.table_index == aggregate.group_index
                    && column.binding.column_index < aggregate.groups.len()
                {
                    Some(NativeOutputSlot::Group(column.binding.column_index))
                } else if column.binding.table_index == aggregate.aggregate_index
                    && column.binding.column_index < aggregate.aggregates.len()
                {
                    Some(NativeOutputSlot::Aggregate(column.binding.column_index))
                } else {
                    None
                }
            }
            Expression::Constant(_) => Some(NativeOutputSlot::Constant),
            _ => None,
        }
    }

    fn extend_positional_bindings(
        bindings: &mut HashMap<ColumnBinding, ColumnBinding>,
        from_table: usize,
        to_table: usize,
        count: usize,
    ) {
        bindings.extend((0..count).map(|ordinal| {
            (
                ColumnBinding::new(from_table, ordinal),
                ColumnBinding::new(to_table, ordinal),
            )
        }));
    }

    fn remap_expression(
        expression: &Expression,
        bindings: &HashMap<ColumnBinding, ColumnBinding>,
    ) -> Option<Expression> {
        let valid = std::cell::Cell::new(true);
        let expression = expression.clone().replace_column_ref(&|column| {
            if column.depth != 0 {
                valid.set(false);
                return None;
            }
            match bindings.get(&column.binding) {
                Some(binding) => Some(Expression::ColumnRef(
                    paro_planner::expression::ColumnRefExpression::new(
                        *binding,
                        column.return_type.clone(),
                    )
                    .into(),
                )),
                None => {
                    valid.set(false);
                    None
                }
            }
        });
        valid.get().then_some(expression)
    }

    fn expressions_equivalent(left: &Expression, right: &Expression) -> bool {
        let (Expression::Conjunction(left), Expression::Conjunction(right)) = (left, right) else {
            return left.equals(right);
        };
        if left.conjunction_type != right.conjunction_type
            || left.children.len() != right.children.len()
        {
            return false;
        }
        let mut matched = vec![false; right.children.len()];
        left.children.iter().all(|left_child| {
            right
                .children
                .iter()
                .enumerate()
                .find(|(ordinal, right_child)| {
                    !matched[*ordinal] && expressions_equivalent(left_child, right_child)
                })
                .map(|(ordinal, _)| matched[ordinal] = true)
                .is_some()
        })
    }

    fn expression_multisets_equal(left: &[Expression], right: &[Expression]) -> bool {
        if left.len() != right.len() {
            return false;
        }
        let mut matched = vec![false; right.len()];
        left.iter().all(|left_expression| {
            right
                .iter()
                .enumerate()
                .find(|(ordinal, right_expression)| {
                    !matched[*ordinal] && expressions_equivalent(left_expression, right_expression)
                })
                .map(|(ordinal, _)| matched[ordinal] = true)
                .is_some()
        })
    }

    let work_dimension = BudgetDimension::CompositionRuleWorkPerGroup;
    let configured_limit = usize::try_from(
        budget
            .optional_limit(work_dimension)
            .expect("rule-work dimensions have a static limit"),
    )
    .unwrap_or(usize::MAX);
    let root_group = memo.canonical_group(root_group);
    let already_consumed = memo
        .group(root_group)
        .map_or(0, |group| group.ledger.consumed(work_dimension));
    let mut matcher = LocalMatcher {
        memo,
        state,
        limit: configured_limit.saturating_sub(already_consumed),
        work_units: 0,
        reads: BTreeMap::new(),
        limited: false,
        cancellation,
    };
    if !matcher.observe_facts(root_group)? {
        return Ok(PatternBindingSet {
            bindings: Box::new([]),
            reads: Box::new([]),
            work_units: matcher.work_units,
            work_dimension,
            completion: PatternEnumerationCompletion::BudgetLimited {
                enumerated_bindings: 0,
                omitted_at_least: 1,
            },
        });
    }
    let (left_group, right_group) = {
        let root_logical = matcher.logical(root_expression)?;
        let [left_group, right_group] = root_logical.key.children.as_ref() else {
            return Ok(PatternBindingSet {
                bindings: Box::new([]),
                reads: matcher
                    .reads
                    .into_values()
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                work_units: matcher.work_units,
                work_dimension,
                completion: PatternEnumerationCompletion::Complete,
            });
        };
        (*left_group, *right_group)
    };
    let left_skeletons = matcher.union_skeletons(left_group)?;
    let right_skeletons = matcher.union_skeletons(right_group)?;
    debug!(
        target: targets::OPTIMIZER,
        group = root_group.index(),
        left_candidates = left_skeletons.len(),
        right_candidates = right_skeletons.len(),
        reads = matcher.reads.len(),
        work_units = matcher.work_units,
        "enumerated local dimension-sharing pattern"
    );
    let variant_limit = usize::from(budget.max_factorization_variants);
    let mut bindings = Vec::new();
    // Enumerate equal-ranked branch alternatives before the Cartesian fringe.
    // Independent branch frontiers commonly publish corresponding rewrites in
    // the same semantic rank. Row-major enumeration could spend the complete
    // bounded frontier pairing one left alternative with every incompatible
    // right alternative, making closure depend on insertion order.
    let frontier_count = left_skeletons.len().max(right_skeletons.len());
    'frontiers: for frontier in 0..frontier_count {
        let left_row_width = if frontier < left_skeletons.len() {
            (frontier + 1).min(right_skeletons.len())
        } else {
            0
        };
        let right_column_height = if frontier < right_skeletons.len() {
            frontier.min(left_skeletons.len())
        } else {
            0
        };
        let pair_indices = (0..left_row_width)
            .map(|right| (frontier, right))
            .chain((0..right_column_height).map(|left| (left, frontier)));
        for (left_index, right_index) in pair_indices {
            if !matcher.admit_work(1)? {
                break 'frontiers;
            }
            let left_skeleton = &left_skeletons[left_index];
            let right_skeleton = &right_skeletons[right_index];
            let combined = UnionSkeleton::SetOperation {
                group: root_group,
                expression: root_expression,
                left: Box::new(left_skeleton.clone()),
                right: Box::new(right_skeleton.clone()),
            };
            if !matcher.union_dimensions_can_share(&combined)
                || !matcher.union_contract_matches(&combined)
            {
                continue;
            }
            let root = match matcher.union_operand(&combined)? {
                Some(root) => root,
                None if matcher.limited => break 'frontiers,
                None => continue,
            };
            let (root, fingerprint) = root;
            // The output frontier counts semantic matches, not raw shell
            // pairs. Pre-admit the exact native shell work. The contract
            // check above has already compared the immutable Memo operator
            // payloads; do not materialize an OwnedLogicalPlan just to run a
            // duplicate recognizer for every pair.
            if !matcher.admit_work(LocalMatcher::operand_nodes(&root))? {
                break 'frontiers;
            }
            if bindings.len() == variant_limit {
                matcher.limited = true;
                break 'frontiers;
            }
            bindings.push(PatternBinding { root, fingerprint });
        }
    }
    bindings.sort_by_key(|binding| binding.fingerprint);
    bindings.dedup_by_key(|binding| binding.fingerprint);
    let enumerated_bindings = bindings.len();
    debug!(
        target: targets::OPTIMIZER,
        group = root_group.index(),
        enumerated_bindings,
        work_units = matcher.work_units,
        budget_limited = matcher.limited,
        "completed local dimension-sharing bindings"
    );
    Ok(PatternBindingSet {
        bindings: bindings.into_boxed_slice(),
        reads: matcher
            .reads
            .into_values()
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        work_units: matcher.work_units,
        work_dimension,
        completion: if matcher.limited {
            PatternEnumerationCompletion::BudgetLimited {
                enumerated_bindings,
                omitted_at_least: 1,
            }
        } else {
            PatternEnumerationCompletion::Complete
        },
    })
}

/// Enumerate exact logical alternatives consumed by a planner transformation.
/// Every visited group revision is returned even when later rule recognition
/// declines, which gives no-match bindings an incremental wake-up edge.
#[cfg(test)]
pub(super) fn pattern_bindings(
    root_group: GroupId,
    root_expression: LogicalExprId,
    memo: &Memo,
    budget: &SearchBudget,
    work_dimension: BudgetDimension,
    cancellation: Option<&paro_context::StatementCancellation>,
) -> Result<PatternBindingSet> {
    let bindings = enumerate_pattern_bindings(
        root_group,
        root_expression,
        memo,
        budget,
        work_dimension,
        cancellation,
        PatternSpec {
            state: None,
            scope: PatternScope::TestSubtree,
            witness: None,
        },
    )?;
    Ok(bindings)
}

/// A pattern owns only the operators whose semantics the rule reads. Opaque
/// inputs retain their group identity, facts and statistics without subscribing
/// to (or multiplying by) equivalent implementations below that boundary.
#[derive(Debug, Clone, Copy)]
enum PatternScope {
    #[cfg(test)]
    TestSubtree,
    Hole,
    Shell,
    PredicateTransfer,
    KeyDomainTransfer,
    TopN,
    Order,
    SearchInput,
    MarkConsumer,
    MarkFilter,
    MarkJoin,
    LimitProjection,
    Projection,
    Preaggregate,
    LeftJoin,
    NonNullAggregate,
    NonNullInput,
    JoinRegion,
    AggregateRegion,
    DimensionRegion,
    LatePayload,
    LateProjection,
    LateAggregate,
    RowIdPath,
    SubsumptionAggregate,
    SubsumptionInput,
    PostReductionPath,
    PostReductionBranch,
    PostReductionWrapper,
    PostReductionScalar,
    PostReductionReduction,
    PostReductionSource,
    ScalarAggregatePath,
    OuterJoinPath,
    CteInlineOwner,
    CteDemandOwner,
    CteDemandReferencePath,
    CteDemandInput,
    CteReferencePath,
}

impl PatternScope {
    /// Return the exact operator types that can produce a non-hole node at
    /// this scope. `None` means that this scope intentionally accepts an
    /// opaque atom for all operators, so the complete frontier is required.
    /// The index is only a read accelerator; callers still run the original
    /// structural matcher for every returned expression.
    fn candidate_operator_types(self) -> Option<&'static [LogicalOperatorType]> {
        use LogicalOperatorType as Op;
        match self {
            Self::PredicateTransfer | Self::MarkFilter => Some(&[Op::Filter]),
            Self::KeyDomainTransfer | Self::MarkJoin => Some(&[Op::ComparisonJoin]),
            Self::CteInlineOwner | Self::CteDemandOwner => Some(&[Op::MaterializedCTE]),
            Self::AggregateRegion
            | Self::Preaggregate
            | Self::NonNullAggregate
            | Self::SubsumptionAggregate => Some(&[Op::Aggregate]),
            Self::LimitProjection | Self::TopN => Some(&[Op::Limit]),
            Self::LateProjection | Self::Projection => Some(&[Op::Projection]),
            Self::NonNullInput => Some(&[Op::Filter, Op::Order, Op::TopN, Op::Limit, Op::Get]),
            Self::Order => Some(&[Op::Projection, Op::Order]),
            Self::MarkConsumer => Some(&[Op::Projection, Op::Filter]),
            Self::LatePayload => Some(&[Op::Projection, Op::TopN]),
            Self::PostReductionPath => Some(&[Op::Projection, Op::Filter, Op::CrossProduct]),
            Self::PostReductionBranch => Some(&[Op::Aggregate, Op::Projection]),
            Self::PostReductionWrapper | Self::PostReductionReduction => Some(&[Op::Aggregate]),
            Self::PostReductionScalar => Some(&[Op::Projection]),
            Self::PostReductionSource => Some(&[Op::Get]),
            Self::RowIdPath => Some(&[
                Op::Get,
                Op::Filter,
                Op::Window,
                Op::Order,
                Op::Limit,
                Op::TopN,
                Op::EmptyResult,
                Op::ComparisonJoin,
                Op::AnyJoin,
                Op::CrossProduct,
            ]),
            // These scopes either treat every unknown operator as an opaque
            // atom or inspect a path witness before choosing child scopes.
            Self::Hole
            | Self::Shell
            | Self::JoinRegion
            | Self::DimensionRegion
            | Self::SearchInput
            | Self::LeftJoin
            | Self::SubsumptionInput
            | Self::LateAggregate
            | Self::ScalarAggregatePath
            | Self::OuterJoinPath
            | Self::CteDemandReferencePath
            | Self::CteDemandInput
            | Self::CteReferencePath => None,
            #[cfg(test)]
            Self::TestSubtree => None,
        }
    }

    fn children(self, operator: &LogicalOperator<()>) -> Option<Vec<Self>> {
        let mut arity = 0;
        operator.visit_child_links(&mut |_| arity += 1);
        let repeat = |scope| Some(vec![scope; arity]);
        match self {
            #[cfg(test)]
            Self::TestSubtree => repeat(Self::TestSubtree),
            Self::ScalarAggregatePath | Self::OuterJoinPath | Self::CteReferencePath
            | Self::CteDemandReferencePath => {
                unreachable!("witness paths choose child scopes from Memo facts")
            }
            Self::CteInlineOwner => matches!(operator, LogicalOperator::MaterializedCTE(_))
                .then(|| vec![Self::Hole, Self::CteReferencePath]),
            Self::CteDemandOwner => matches!(operator, LogicalOperator::MaterializedCTE(_))
                .then(|| vec![Self::Hole, Self::CteDemandReferencePath]),
            Self::CteDemandInput => match operator {
                LogicalOperator::Projection(_) | LogicalOperator::Filter(_) => repeat(Self::CteDemandInput),
                LogicalOperator::Get(_) | LogicalOperator::ExpressionGet(_) | LogicalOperator::EmptyResult(_) => repeat(Self::Hole),
                _ => None,
            },
            Self::Hole => unreachable!("group holes do not inspect operators"),
            Self::Shell => repeat(Self::Hole),
            Self::PredicateTransfer => matches!(operator, LogicalOperator::Filter(_)).then(|| vec![Self::Shell]),
            Self::KeyDomainTransfer => matches!(operator, LogicalOperator::Join(Join::Comparison(join)) if join.join_type == JoinType::Semi).then(|| vec![Self::Shell, Self::Hole]),
            Self::SubsumptionAggregate => matches!(operator, LogicalOperator::Aggregate(_)).then(|| vec![Self::SubsumptionInput]),
            // Detail subsumption consumes a clean join region, projection /
            // filter exposure paths, and one partial-aggregate shell. It
            // does not inspect arbitrary relations hanging off that region.
            Self::SubsumptionInput => match operator {
                LogicalOperator::Projection(_) | LogicalOperator::Filter(_) => repeat(Self::SubsumptionInput),
                LogicalOperator::Join(Join::Comparison(join)) if matches!(join.join_type, JoinType::Inner | JoinType::Semi | JoinType::RightSemi) => repeat(Self::SubsumptionInput),
                LogicalOperator::Aggregate(_) => repeat(Self::Shell),
                _ => repeat(Self::Hole),
            },
            // AggregatePostReduction has a deliberately small native path
            // grammar.  It exposes both sides of the cross product and the
            // exact scalar wrapper, while the two source Gets remain the
            // selected Memo expressions.  Richer source paths continue to
            // use the authoritative owned recognizer.
            Self::PostReductionPath => match operator {
                LogicalOperator::Projection(_) | LogicalOperator::Filter(_) => {
                    repeat(Self::PostReductionPath)
                }
                LogicalOperator::Join(Join::Cross(_)) => {
                    Some(vec![Self::PostReductionBranch; arity])
                }
                _ => None,
            },
            Self::PostReductionBranch => match operator {
                LogicalOperator::Aggregate(aggregate) if !aggregate.groups.is_empty() => {
                    Some(vec![Self::PostReductionSource])
                }
                LogicalOperator::Projection(_) => Some(vec![Self::PostReductionWrapper]),
                _ => None,
            },
            Self::PostReductionWrapper => {
                matches!(operator, LogicalOperator::Aggregate(_))
                    .then(|| vec![Self::PostReductionScalar])
            }
            Self::PostReductionScalar => {
                matches!(operator, LogicalOperator::Projection(_))
                    .then(|| vec![Self::PostReductionReduction])
            }
            Self::PostReductionReduction => {
                matches!(operator, LogicalOperator::Aggregate(_))
                    .then(|| vec![Self::PostReductionSource])
            }
            Self::PostReductionSource => {
                matches!(operator, LogicalOperator::Get(_)).then(|| Vec::new())
            }
            Self::LatePayload => match operator {
                LogicalOperator::Projection(_) => repeat(Self::RowIdPath),
                LogicalOperator::TopN(_) => repeat(Self::LateProjection),
                _ => None,
            },
            Self::LateProjection => matches!(operator, LogicalOperator::Projection(_)).then(|| vec![Self::LateAggregate]),
            Self::LateAggregate => match operator {
                LogicalOperator::Aggregate(_) => repeat(Self::RowIdPath),
                _ => Self::RowIdPath.children(operator),
            },
            // The row-id proof inspects every branch to prove one source
            // occurrence. Unknown operators decline; hiding them behind a
            // schema-only hole could manufacture source uniqueness.
            Self::RowIdPath => match operator {
                LogicalOperator::Get(_) | LogicalOperator::Filter(_) | LogicalOperator::Window(_)
                | LogicalOperator::Order(_) | LogicalOperator::Limit(_) | LogicalOperator::TopN(_)
                | LogicalOperator::EmptyResult(_) | LogicalOperator::Join(_) => repeat(Self::RowIdPath),
                _ => None,
            },
            Self::JoinRegion => match operator {
                LogicalOperator::Filter(_) => repeat(Self::JoinRegion),
                LogicalOperator::Join(join) if crate::join_order::relation_manager::RelationManager::join_shell_is_reorderable(join)
                    || matches!(join, Join::Comparison(join) if crate::join_order::relation_manager::RelationManager::reduction_join_shell_is_reorderable(join)) => repeat(Self::JoinRegion),
                _ => repeat(Self::Hole),
            },
            Self::AggregateRegion => matches!(operator, LogicalOperator::Aggregate(_)).then(|| vec![Self::DimensionRegion]),
            Self::DimensionRegion => match operator {
                LogicalOperator::Projection(_) => repeat(Self::DimensionRegion),
                LogicalOperator::Join(Join::Comparison(join)) if join.join_type == JoinType::Inner && join.duplicate_eliminated_columns.is_empty() && !join.delim_flipped => repeat(Self::DimensionRegion),
                _ => repeat(Self::Hole),
            },
            Self::LimitProjection => matches!(operator, LogicalOperator::Limit(_)).then(|| vec![Self::Projection]),
            Self::Projection => matches!(operator, LogicalOperator::Projection(_)).then(|| vec![Self::Hole]),
            Self::Preaggregate => matches!(operator, LogicalOperator::Aggregate(aggregate) if aggregate.groups.len() == 1).then(|| vec![Self::LeftJoin]),
            Self::LeftJoin => matches!(operator, LogicalOperator::Join(Join::Comparison(join)) if join.join_type == JoinType::Left).then(|| vec![Self::Shell; arity]),
            Self::NonNullAggregate => matches!(operator, LogicalOperator::Aggregate(_)).then(|| vec![Self::NonNullInput]),
            Self::NonNullInput => match operator {
                LogicalOperator::Filter(_) | LogicalOperator::Order(_) | LogicalOperator::TopN(_) | LogicalOperator::Limit(_) => repeat(Self::NonNullInput),
                LogicalOperator::Get(_) => repeat(Self::Hole),
                _ => None,
            },
            Self::TopN => matches!(operator, LogicalOperator::Limit(_)).then(|| vec![Self::Order]),
            Self::Order => match operator {
                LogicalOperator::Projection(_) => Some(vec![Self::Order]),
                LogicalOperator::Order(_) => Some(vec![Self::SearchInput]),
                _ => None,
            },
            Self::SearchInput => match operator {
                LogicalOperator::Projection(_) | LogicalOperator::Filter(_) => repeat(Self::SearchInput),
                _ => repeat(Self::Hole),
            },
            Self::MarkConsumer => match operator {
                LogicalOperator::Projection(_) => Some(vec![Self::MarkFilter]),
                LogicalOperator::Filter(_) => Self::MarkFilter.children(operator),
                _ => None,
            },
            Self::MarkFilter => match operator {
                LogicalOperator::Filter(filter) if matches!(filter.expressions.as_slice(), [Expression::ColumnRef(_)]) => Some(vec![Self::MarkJoin]),
                _ => None,
            },
            Self::MarkJoin => match operator {
                LogicalOperator::Join(Join::Comparison(join)) if join.join_type == JoinType::Mark => repeat(Self::Hole),
                _ => None,
            },
        }
    }
}

pub(super) fn scoped_pattern_bindings(
    transformation: PlannerTransformation,
    root_group: GroupId,
    root_expression: LogicalExprId,
    memo: &Memo,
    state: &PlannerTransformState,
    cancellation: Option<&paro_context::StatementCancellation>,
    work_dimension: BudgetDimension,
) -> Result<PatternBindingSet> {
    let scope = match transformation {
        PlannerTransformation::PredicateTransfer => PatternScope::PredicateTransfer,
        PlannerTransformation::KeyDomainTransfer => PatternScope::KeyDomainTransfer,
        PlannerTransformation::TopNIntroduction => PatternScope::TopN,
        PlannerTransformation::LimitPushdown => PatternScope::LimitProjection,
        PlannerTransformation::LatePayloadFetch => PatternScope::LatePayload,
        PlannerTransformation::AggregateJoinPreaggregation => PatternScope::Preaggregate,
        PlannerTransformation::AggregateJoinSubsumption => PatternScope::SubsumptionAggregate,
        PlannerTransformation::AggregateNonNullInput => PatternScope::NonNullAggregate,
        PlannerTransformation::JoinRegionEnumeration => PatternScope::JoinRegion,
        PlannerTransformation::AggregateDimensionDeferral
        | PlannerTransformation::AggregateInputMaterialization => PatternScope::AggregateRegion,
        PlannerTransformation::MarkJoinToSemi => PatternScope::MarkConsumer,
        PlannerTransformation::AggregatePostReduction => PatternScope::PostReductionPath,
        PlannerTransformation::ScalarAggregateWindow => PatternScope::ScalarAggregatePath,
        PlannerTransformation::JoinElimination => PatternScope::OuterJoinPath,
        PlannerTransformation::CtePartitionedMaterialization
        | PlannerTransformation::CteInline
        | PlannerTransformation::CteFilterPushdown => PatternScope::CteInlineOwner,
        PlannerTransformation::CteDemandPushdown => PatternScope::CteDemandOwner,
        PlannerTransformation::AggregateDimensionSharing => {
            unreachable!("dimension sharing has its own native binding")
        }
    };
    let witness = match transformation {
        PlannerTransformation::ScalarAggregateWindow => Some(PatternWitness::ScalarAggregate),
        PlannerTransformation::JoinElimination => Some(PatternWitness::OuterJoin),
        _ => None,
    };
    let mut bindings = enumerate_pattern_bindings(
        root_group,
        root_expression,
        memo,
        memo.budget(),
        work_dimension,
        cancellation,
        PatternSpec {
            state: Some(state),
            scope,
            witness,
        },
    )?;

    if matches!(
        transformation,
        PlannerTransformation::AggregateDimensionDeferral
    ) {
        // The aggregate-region matcher deliberately treats a non-dimension
        // shell as an opaque hole.  For deferral, however, the aggregate's
        // direct input is the boundary at which a newly published logical
        // alternative can expose the deferred dimension shape.  Subscribe to
        // that input frontier during discovery, before the first binding is
        // applied; application-only reads are too late to wake this task for
        // an alternative published in the same early planning wave.
        let mut reads = bindings.reads.into_vec();
        for child in memo
            .logical_expr(root_expression)
            .ok_or_else(|| paro_error::internal("aggregate deferral lost root expression"))?
            .key
            .children
            .iter()
            .copied()
        {
            let child = memo.canonical_group(child);
            let frontier = PatternRead::from_group(memo, child)?;
            if let Some(read) = reads.iter_mut().find(|read| read.group == child) {
                *read = frontier;
            } else {
                reads.push(frontier);
            }
        }
        bindings.reads = reads.into_boxed_slice();
    }

    Ok(bindings)
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PatternWitness {
    ScalarAggregate,
    OuterJoin,
    CteReference,
}

impl PatternWitness {
    fn stable_tag(self) -> u8 {
        match self {
            Self::ScalarAggregate => 0,
            Self::OuterJoin => 1,
            Self::CteReference => 2,
        }
    }

    fn matches<Child>(self, operator: &LogicalOperator<Child>) -> bool {
        match (self, operator) {
            (Self::ScalarAggregate, LogicalOperator::Aggregate(aggregate)) => {
                aggregate.groups.is_empty()
            }
            (Self::OuterJoin, LogicalOperator::Join(Join::Comparison(join))) => {
                matches!(join.join_type, JoinType::Left | JoinType::Right)
            }
            (Self::CteReference, LogicalOperator::CTERef(_)) => true,
            _ => false,
        }
    }
}

fn transformation_root_witness(transformation: PlannerTransformation) -> Option<PatternWitness> {
    match transformation {
        PlannerTransformation::AggregatePostReduction
        | PlannerTransformation::ScalarAggregateWindow => Some(PatternWitness::ScalarAggregate),
        PlannerTransformation::JoinElimination => Some(PatternWitness::OuterJoin),
        _ => None,
    }
}

/// Return an exact negative root dispatch when every child path has a
/// completed, still-current witness lookup.  A positive lookup is not enough
/// to dispatch the rule: the full matcher still has to check the operator
/// path and construct its exact binding.  Missing or stale entries likewise
/// fall back to ordinary matching, so a newly published alternative cannot
/// be hidden by this fast path.
pub(super) fn cached_negative_root_reads(
    transformation: PlannerTransformation,
    root_group: GroupId,
    root_expression: LogicalExprId,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<Option<Box<[PatternRead]>>> {
    let Some(witness) = transformation_root_witness(transformation) else {
        return Ok(None);
    };
    let logical = memo
        .logical_expr(root_expression)
        .ok_or_else(|| paro_error::internal("root dispatch lost logical expression"))?;
    let mut reads = BTreeSet::from([PatternRead::facts_from_group(memo, root_group)?]);
    let mut cache = state.witness_cache.lock().expect("witness cache poisoned");
    for child in logical.key.children.iter().copied() {
        let child = memo.canonical_group(child);
        let key = (child, witness.stable_tag());
        let Some(entry) = cache.entries.get(&key).cloned() else {
            return Ok(None);
        };
        let current = entry
            .reads
            .iter()
            .try_fold(true, |current, read| -> Result<bool> {
                Ok(current && read.is_current(memo)?)
            })?;
        if !current {
            cache.entries.remove(&key);
            cache.invalidations = cache.invalidations.saturating_add(1);
            return Ok(None);
        }
        cache.hits = cache.hits.saturating_add(1);
        if entry.matches {
            // A positive child witness only proves that this root may match;
            // it does not prove that the complete scoped binding is valid.
            return Ok(None);
        }
        reads.extend(entry.reads.iter().copied());
    }
    cache.root_dispatch_skips = cache.root_dispatch_skips.saturating_add(1);
    Ok(Some(
        reads.into_iter().collect::<Vec<_>>().into_boxed_slice(),
    ))
}

struct PatternSpec<'a> {
    state: Option<&'a PlannerTransformState>,
    scope: PatternScope,
    witness: Option<PatternWitness>,
}

fn enumerate_pattern_bindings(
    root_group: GroupId,
    root_expression: LogicalExprId,
    memo: &Memo,
    budget: &SearchBudget,
    work_dimension: BudgetDimension,
    cancellation: Option<&paro_context::StatementCancellation>,
    pattern: PatternSpec<'_>,
) -> Result<PatternBindingSet> {
    let PatternSpec {
        state,
        scope,
        witness,
    } = pattern;
    struct Enumerator<'a> {
        memo: &'a Memo,
        state: Option<&'a PlannerTransformState>,
        limit: usize,
        work_units: usize,
        reads: BTreeMap<GroupId, PatternRead>,
        read_log: Vec<GroupId>,
        witness_groups: BTreeMap<(GroupId, PatternWitness), bool>,
        // Invocation-local negative DAG results. No Memo mutation can occur
        // during enumeration; reads from the first traversal remain subscribed.
        empty_non_null_inputs: BTreeSet<GroupId>,
        recursion_cuts: usize,
        limited: bool,
        cancellation: Option<&'a paro_context::StatementCancellation>,
    }

    impl Enumerator<'_> {
        /// Existential pattern lookahead over a DAG. A negative match records
        /// all inspected frontiers, so adding a qualifying non-selected
        /// expression wakes the rule. Admission precedes each inspected node.
        fn contains_witness(
            &mut self,
            root: LogicalExprId,
            witness: PatternWitness,
        ) -> Result<bool> {
            let mut pending = vec![root];
            let mut visited = BTreeSet::new();
            while let Some(expression) = pending.pop() {
                if !self.admit_work(1)? {
                    return Ok(false);
                }
                let logical = self
                    .memo
                    .logical_expr(expression)
                    .ok_or_else(|| paro_error::internal("pattern lookahead lost expression"))?;
                let operator = &self
                    .state
                    .and_then(|state| state.payloads.logical.get(logical.payload.index()))
                    .ok_or_else(|| paro_error::internal("pattern lookahead has no operator shell"))?
                    .semantic_template
                    .operator;
                if witness.matches(operator) {
                    return Ok(true);
                }
                for child in logical.key.children.iter().copied() {
                    let child = self.memo.canonical_group(child);
                    if visited.insert(child) {
                        if !self.observe(child)? {
                            return Ok(false);
                        }
                        if self.work_units >= self.limit {
                            self.limited = true;
                            return Ok(false);
                        }
                        pending.extend(
                            self.memo
                                .group(child)
                                .ok_or_else(|| {
                                    paro_error::internal("pattern lookahead lost group")
                                })?
                                .logical_exprs()
                                .iter()
                                .copied(),
                        );
                    }
                }
            }
            Ok(false)
        }

        fn group_contains_witness(
            &mut self,
            group: GroupId,
            witness: PatternWitness,
        ) -> Result<bool> {
            let group = self.memo.canonical_group(group);
            if let Some(matches) = self.witness_groups.get(&(group, witness)) {
                return Ok(*matches);
            }
            let read_start = self.read_log.len();
            let work_start = self.work_units;

            // Nested-path rules repeatedly ask whether the same child group
            // contains an existential witness. Reuse only a completed result
            // whose full dependency transcript is still current. Applying the
            // transcript to this enumerator is necessary for task wakeups;
            // validating a cache entry without registering those reads would
            // make a later witness insertion invisible to the task.
            if let Some(entry) = self.cached_witness(group, witness)? {
                if !self.admit_work(entry.work_units)? {
                    return Ok(false);
                }
                for read in entry.reads.iter().copied() {
                    self.register_cached_read(read)?;
                }
                self.witness_groups.insert((group, witness), entry.matches);
                return Ok(entry.matches);
            }
            if !self.observe(group)? {
                return Ok(false);
            }
            if self.work_units >= self.limit {
                self.limited = true;
                return Ok(false);
            }
            let expressions = self
                .memo
                .group(group)
                .ok_or_else(|| paro_error::internal("pattern witness lost group"))?
                .logical_exprs()
                .to_vec();
            for expression in expressions {
                if self.contains_witness(expression, witness)? {
                    self.witness_groups.insert((group, witness), true);
                    self.store_witness(group, witness, true, read_start, work_start);
                    return Ok(true);
                }
            }
            if !self.limited {
                self.witness_groups.insert((group, witness), false);
                self.store_witness(group, witness, false, read_start, work_start);
            }
            Ok(false)
        }

        fn cached_witness(
            &mut self,
            group: GroupId,
            witness: PatternWitness,
        ) -> Result<Option<CachedPatternWitness>> {
            let Some(state) = self.state else {
                return Ok(None);
            };
            let mut cache = state.witness_cache.lock().expect("witness cache poisoned");
            let key = (group, witness.stable_tag());
            let Some(entry) = cache.entries.get(&key).cloned() else {
                return Ok(None);
            };
            let current = entry
                .reads
                .iter()
                .try_fold(true, |current, read| -> Result<bool> {
                    Ok(current && read.is_current(self.memo)?)
                })?;
            if current {
                cache.hits = cache.hits.saturating_add(1);
                Ok(Some(entry))
            } else {
                cache.entries.remove(&key);
                cache.invalidations = cache.invalidations.saturating_add(1);
                Ok(None)
            }
        }

        fn store_witness(
            &self,
            group: GroupId,
            witness: PatternWitness,
            matches: bool,
            read_start: usize,
            work_start: usize,
        ) {
            let Some(state) = self.state else {
                return;
            };
            let mut read_groups = BTreeSet::from([group]);
            read_groups.extend(self.read_log[read_start..].iter().copied());
            let reads = read_groups
                .into_iter()
                .filter_map(|group| self.reads.get(&group).copied())
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let mut cache = state.witness_cache.lock().expect("witness cache poisoned");
            cache.entries.insert(
                (group, witness.stable_tag()),
                CachedPatternWitness {
                    matches,
                    work_units: self.work_units.saturating_sub(work_start),
                    reads,
                },
            );
        }

        fn register_cached_read(&mut self, read: PatternRead) -> Result<()> {
            let group = self.memo.canonical_group(read.group);
            if let Some(current) = self.reads.get_mut(&group) {
                if current.logical_frontier_revision.is_none()
                    && read.logical_frontier_revision.is_some()
                {
                    *current = PatternRead::from_group(self.memo, group)?;
                    self.read_log.push(group);
                }
            } else {
                self.reads.insert(
                    group,
                    if read.logical_frontier_revision.is_some() {
                        PatternRead::from_group(self.memo, group)?
                    } else {
                        PatternRead::facts_from_group(self.memo, group)?
                    },
                );
                self.read_log.push(group);
            }
            Ok(())
        }
        fn operand_work(operand: &PatternOperand) -> usize {
            match operand {
                PatternOperand::Group(_) => 1,
                PatternOperand::Expression { children, .. } => {
                    1usize.saturating_add(children.iter().map(Self::operand_work).sum::<usize>())
                }
            }
        }

        fn admit_work(&mut self, units: usize) -> Result<bool> {
            if !self.memo.control().checkpoint()? {
                self.limited = true;
                return Ok(false);
            }
            if let Some(cancellation) = self.cancellation {
                cancellation.check()?;
            }
            if self.work_units.saturating_add(units) > self.limit {
                self.limited = true;
                return Ok(false);
            }
            self.work_units += units;
            Ok(true)
        }

        fn observe(&mut self, group: GroupId) -> Result<bool> {
            let group = self.memo.canonical_group(group);
            if let Some(read) = self.reads.get_mut(&group) {
                if read.logical_frontier_revision.is_none() {
                    *read = PatternRead::from_group(self.memo, group)?;
                    self.read_log.push(group);
                }
                return Ok(true);
            }
            if !self.admit_work(1)? {
                return Ok(false);
            }
            self.reads
                .insert(group, PatternRead::from_group(self.memo, group)?);
            self.read_log.push(group);
            Ok(true)
        }

        fn observe_facts(&mut self, group: GroupId) -> Result<bool> {
            let group = self.memo.canonical_group(group);
            if self.reads.contains_key(&group) {
                return Ok(true);
            }
            if !self.admit_work(1)? {
                return Ok(false);
            }
            self.reads
                .insert(group, PatternRead::facts_from_group(self.memo, group)?);
            self.read_log.push(group);
            Ok(true)
        }

        fn group(
            &mut self,
            group: GroupId,
            active: &mut BTreeSet<GroupId>,
            scope: PatternScope,
        ) -> Result<Vec<(PatternOperand, Fingerprint)>> {
            // Shared DAG revisits may reuse an existing PatternRead without
            // admitting new ledger work. They still consume wall time: a
            // negative match must not hide cancellation behind read reuse.
            if !self.admit_work(0)? {
                return Ok(Vec::new());
            }
            if let Some(cancellation) = self.cancellation {
                cancellation.check()?;
            }
            if matches!(scope, PatternScope::NonNullInput) && !self.admit_work(1)? {
                return Ok(Vec::new());
            }
            let group = self.memo.canonical_group(group);
            let group_ref = self.memo.group(group).ok_or_else(|| {
                paro_error::internal("pattern matcher references an unknown Memo group")
            })?;
            // Do not materialize or sort a frontier after the work budget has
            // already been exhausted.  Admission must precede copying the
            // expression ids so a large shared DAG cannot hide work behind a
            // Complete result.
            if self.work_units >= self.limit {
                self.limited = true;
                return Ok(Vec::new());
            }
            if matches!(scope, PatternScope::Hole) {
                if !self.observe_facts(group)? || !self.admit_work(1)? {
                    return Ok(Vec::new());
                }
                let mut fingerprint = StableFingerprintBuilder::default();
                fingerprint.write_bytes(b"paro.pattern.semantic-group-hole.v1");
                fingerprint.write_u64(group.0 as u64);
                fingerprint.write_fingerprint(group_ref.logical_fact_fingerprint());
                fingerprint.write_fingerprint(group_ref.statistics_snapshot_fingerprint());
                return Ok(vec![(PatternOperand::Group(group), fingerprint.finish())]);
            }
            if !self.observe(group)? {
                return Ok(Vec::new());
            }
            if !active.insert(group) {
                self.recursion_cuts = self.recursion_cuts.saturating_add(1);
                // A CTE ownership proof must enumerate every occurrence in
                // its consumer scope. A recursion cut is not evidence that a
                // subtree contains no reference; decline this cyclic binding
                // and let finite alternatives establish the requirement.
                if matches!(
                    scope,
                    PatternScope::CteReferencePath | PatternScope::CteDemandReferencePath
                ) {
                    return Ok(Vec::new());
                }
                if !self.admit_work(1)? {
                    return Ok(Vec::new());
                }
                let mut fingerprint = StableFingerprintBuilder::default();
                fingerprint.write_bytes(b"paro.pattern.group-hole.v1");
                fingerprint.write_u64(group.0 as u64);
                return Ok(vec![(PatternOperand::Group(group), fingerprint.finish())]);
            }
            if matches!(scope, PatternScope::NonNullInput)
                && self.empty_non_null_inputs.contains(&group)
            {
                active.remove(&group);
                return Ok(Vec::new());
            }
            let recursion_cuts = self.recursion_cuts;
            let mut expressions = if let (Some(_state), Some(operator_types)) =
                (self.state, scope.candidate_operator_types())
            {
                let mut indexed = BTreeSet::new();
                let mut expressions = Vec::new();
                for operator_type in operator_types {
                    for expression in group_ref
                        .logical_exprs_of_operator(super::super::identity::operator_tag(
                            *operator_type,
                        ))
                        .iter()
                        .copied()
                    {
                        indexed.insert(expression);
                        expressions.push(expression);
                    }
                }
                // Core-only callers and hand-built planner tests may insert
                // an expression without a semantic tag. Keep that bucket in
                // the indexed path and let the original operator check below
                // reject it when its type is not allowed.
                expressions.extend(
                    group_ref
                        .logical_exprs_of_operator(
                            crate::cascades::memo::UNTYPED_LOGICAL_OPERATOR_TAG,
                        )
                        .iter()
                        .copied(),
                );
                expressions
            } else {
                group_ref.logical_exprs().to_vec()
            };
            expressions.sort_by_key(|expression| {
                self.memo
                    .logical_expr(*expression)
                    .map(|expression| expression.key.stable_fingerprint())
                    .unwrap_or_default()
            });
            let mut result = Vec::new();
            for expression in expressions {
                for candidate in self.expression(group, expression, active, scope)? {
                    if result.len() == self.limit {
                        self.limited = true;
                        break;
                    }
                    result.push(candidate);
                }
                if self.limited {
                    break;
                }
            }
            active.remove(&group);
            result.sort_by_key(|(_, fingerprint)| *fingerprint);
            result.dedup_by_key(|(_, fingerprint)| *fingerprint);
            // NonNullInput follows only unary transparent shells to Get.
            // A fully inspected failure is independent of the caller's path.
            // Never reuse truncation or a recursion-cut result as absence.
            if matches!(scope, PatternScope::NonNullInput)
                && result.is_empty()
                && !self.limited
                && recursion_cuts == self.recursion_cuts
            {
                self.empty_non_null_inputs.insert(group);
            }
            Ok(result)
        }

        fn expression(
            &mut self,
            group: GroupId,
            expression: LogicalExprId,
            active: &mut BTreeSet<GroupId>,
            scope: PatternScope,
        ) -> Result<Vec<(PatternOperand, Fingerprint)>> {
            if !self.admit_work(0)? {
                return Ok(Vec::new());
            }
            if let Some(cancellation) = self.cancellation {
                cancellation.check()?;
            }
            if matches!(scope, PatternScope::NonNullInput) && !self.admit_work(1)? {
                return Ok(Vec::new());
            }
            if self.work_units >= self.limit {
                self.limited = true;
                return Ok(Vec::new());
            }
            let logical = self.memo.logical_expr(expression).ok_or_else(|| {
                paro_error::internal("pattern matcher references an unknown logical expression")
            })?;
            let child_scopes = if self.state.is_none() {
                vec![scope; logical.key.children.len()]
            } else {
                let operator = &self
                    .state
                    .and_then(|state| state.payloads.logical.get(logical.payload.index()))
                    .ok_or_else(|| paro_error::internal("scoped pattern has no operator shell"))?
                    .semantic_template
                    .operator;
                let path_witness = match scope {
                    PatternScope::ScalarAggregatePath => Some(PatternWitness::ScalarAggregate),
                    PatternScope::OuterJoinPath => Some(PatternWitness::OuterJoin),
                    PatternScope::CteReferencePath | PatternScope::CteDemandReferencePath => {
                        Some(PatternWitness::CteReference)
                    }
                    _ => None,
                };
                if let Some(witness) = path_witness {
                    if witness.matches(operator) {
                        vec![PatternScope::Hole; logical.key.children.len()]
                    } else {
                        let children = logical.key.children.to_vec();
                        let mut scopes = Vec::with_capacity(children.len());
                        let mut found = false;
                        for child in children {
                            if self.group_contains_witness(child, witness)? {
                                scopes.push(scope);
                                found = true;
                            } else {
                                scopes.push(
                                    if matches!(scope, PatternScope::CteDemandReferencePath) {
                                        PatternScope::CteDemandInput
                                    } else {
                                        PatternScope::Hole
                                    },
                                );
                            }
                        }
                        if !found {
                            return Ok(Vec::new());
                        }
                        scopes
                    }
                } else {
                    if matches!(scope, PatternScope::JoinRegion)
                        && !matches!(
                            operator,
                            LogicalOperator::Get(_) | LogicalOperator::Filter(_)
                        )
                        && !matches!(operator, LogicalOperator::Join(join) if crate::join_order::relation_manager::RelationManager::join_shell_is_reorderable(join)
                        || matches!(join, Join::Comparison(join) if crate::join_order::relation_manager::RelationManager::reduction_join_shell_is_reorderable(join)))
                    {
                        if active.len() == 1 {
                            return Ok(Vec::new());
                        }
                        if !self.observe_facts(group)? || !self.admit_work(1)? {
                            return Ok(Vec::new());
                        }
                        let mut fingerprint = StableFingerprintBuilder::default();
                        fingerprint.write_bytes(b"paro.pattern.join-atom.v1");
                        fingerprint.write_u64(group.0 as u64);
                        return Ok(vec![(PatternOperand::Group(group), fingerprint.finish())]);
                    }
                    if matches!(scope, PatternScope::SubsumptionInput)
                        && !matches!(
                            operator,
                            LogicalOperator::Get(_)
                                | LogicalOperator::Filter(_)
                                | LogicalOperator::Projection(_)
                                | LogicalOperator::Aggregate(_)
                                | LogicalOperator::Join(Join::Comparison(_))
                        )
                    {
                        if !self.observe_facts(group)? || !self.admit_work(1)? {
                            return Ok(Vec::new());
                        }
                        let mut fingerprint = StableFingerprintBuilder::default();
                        fingerprint.write_bytes(b"paro.pattern.subsumption-input-hole.v1");
                        fingerprint.write_u64(group.0 as u64);
                        return Ok(vec![(PatternOperand::Group(group), fingerprint.finish())]);
                    }
                    // TopN implementation matching consumes its local search
                    // access path too, not just the ordering shell. Other inputs
                    // remain opaque rather than expanding unrelated join trees.
                    if matches!(scope, PatternScope::SearchInput)
                        && !matches!(
                            operator,
                            LogicalOperator::Projection(_)
                                | LogicalOperator::Filter(_)
                                | LogicalOperator::Get(_)
                                | LogicalOperator::SearchScan(_)
                                | LogicalOperator::FullTextFilterScan(_)
                        )
                    {
                        if !self.observe_facts(group)? || !self.admit_work(1)? {
                            return Ok(Vec::new());
                        }
                        let mut fingerprint = StableFingerprintBuilder::default();
                        fingerprint.write_bytes(b"paro.pattern.search-input-hole.v1");
                        fingerprint.write_u64(group.0 as u64);
                        return Ok(vec![(PatternOperand::Group(group), fingerprint.finish())]);
                    }
                    let Some(scopes) = scope.children(operator) else {
                        return Ok(Vec::new());
                    };
                    scopes
                }
            };
            let mut combinations: Vec<Vec<(PatternOperand, Fingerprint)>> = vec![Vec::new()];
            for (child, scope) in logical.key.children.iter().copied().zip(child_scopes) {
                let alternatives = self.group(child, active, scope)?;
                let mut next = Vec::new();
                'outer: for prefix in combinations {
                    for alternative in &alternatives {
                        if next.len() == self.limit {
                            self.limited = true;
                            break 'outer;
                        }
                        let cloned_work = prefix
                            .iter()
                            .map(|(operand, _)| Self::operand_work(operand))
                            .sum::<usize>()
                            .saturating_add(Self::operand_work(&alternative.0));
                        if !self.admit_work(cloned_work)? {
                            break 'outer;
                        }
                        let mut candidate = prefix.clone();
                        candidate.push(alternative.clone());
                        next.push(candidate);
                    }
                }
                combinations = next;
                if combinations.is_empty() {
                    break;
                }
            }
            if logical.key.children.is_empty() {
                combinations = vec![Vec::new()];
            }
            let mut operands = Vec::with_capacity(combinations.len());
            for children in combinations {
                let cloned_children = children
                    .iter()
                    .map(|(operand, _)| Self::operand_work(operand))
                    .sum::<usize>();
                if !self.admit_work(cloned_children.saturating_add(1))? {
                    break;
                }
                let mut fingerprint = StableFingerprintBuilder::default();
                fingerprint.write_bytes(b"paro.pattern.binding.v1");
                // The operator fingerprint already contains semantic
                // scalar fingerprints. Child group ids are allocation
                // artifacts and are replaced by the exact bound child
                // fingerprints below.
                fingerprint.write_fingerprint(logical.key.operator);
                for (_, child_fingerprint) in &children {
                    fingerprint.write_fingerprint(*child_fingerprint);
                }
                operands.push((
                    PatternOperand::Expression {
                        group,
                        expression,
                        children: children
                            .iter()
                            .map(|(operand, _)| operand.clone())
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                    },
                    fingerprint.finish(),
                ));
            }
            Ok(operands)
        }
    }

    let configured_limit = usize::try_from(
        budget
            .optional_limit(work_dimension)
            .expect("rule-work dimensions have a static limit"),
    )
    .unwrap_or(usize::MAX);
    let already_consumed = memo
        .group(memo.canonical_group(root_group))
        .map_or(0, |group| group.ledger.consumed(work_dimension));
    let limit = configured_limit.saturating_sub(already_consumed);
    let mut enumerator = Enumerator {
        memo,
        state,
        limit,
        work_units: 0,
        reads: BTreeMap::new(),
        read_log: Vec::new(),
        witness_groups: BTreeMap::new(),
        empty_non_null_inputs: BTreeSet::new(),
        recursion_cuts: 0,
        limited: false,
        cancellation,
    };
    let root_group = memo.canonical_group(root_group);
    // The semantic adapter consumes the root group's facts and cardinality,
    // even though root dispatch starts from an already selected expression.
    // Observe it through the same path as every descendant.
    if !enumerator.observe_facts(root_group)? {
        return Ok(PatternBindingSet {
            bindings: Box::new([]),
            reads: Box::new([]),
            work_units: enumerator.work_units,
            work_dimension,
            completion: PatternEnumerationCompletion::BudgetLimited {
                enumerated_bindings: 0,
                omitted_at_least: 1,
            },
        });
    }
    let path_scoped = matches!(
        scope,
        PatternScope::ScalarAggregatePath | PatternScope::OuterJoinPath
    );
    let can_match = match witness {
        Some(_) if path_scoped => true,
        Some(witness) => enumerator.contains_witness(root_expression, witness)?,
        None => true,
    };
    let candidates = if can_match {
        enumerator.expression(
            root_group,
            root_expression,
            &mut BTreeSet::from([root_group]),
            scope,
        )?
    } else {
        Vec::new()
    };
    let mut bindings = candidates
        .into_iter()
        .map(|(root, fingerprint)| PatternBinding { root, fingerprint })
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.fingerprint);
    bindings.dedup_by_key(|binding| binding.fingerprint);
    let enumerated_bindings = bindings.len();
    Ok(PatternBindingSet {
        bindings: bindings.into_boxed_slice(),
        reads: enumerator
            .reads
            .into_values()
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        work_units: enumerator.work_units,
        work_dimension,
        completion: if enumerator.limited {
            PatternEnumerationCompletion::BudgetLimited {
                enumerated_bindings,
                omitted_at_least: 1,
            }
        } else {
            PatternEnumerationCompletion::Complete
        },
    })
}

/// Cheap root dispatch for dimension sharing.
///
/// The full matcher deliberately checks every exact child choice, but the
/// initial quality lane must not run that Cartesian walk for every UNION shell
/// in a Memo.  This probe only answers whether the two root arms can contain
/// the immutable branch skeleton at all.  A negative answer carries every
/// group frontier read by the probe, so a later publication invalidates the
/// answer and re-enqueues the exact task.  A positive answer is advisory and
/// still goes through `dimension_sharing_pattern_bindings`.
pub(super) fn dimension_sharing_root_dispatch(
    root_group: GroupId,
    root_expression: LogicalExprId,
    memo: &Memo,
    state: &PlannerTransformState,
) -> Result<RootDispatch> {
    struct Probe<'a> {
        memo: &'a Memo,
        state: &'a PlannerTransformState,
        reads: BTreeMap<GroupId, PatternRead>,
        visiting: BTreeSet<GroupId>,
    }

    impl Probe<'_> {
        fn observe(&mut self, group: GroupId) -> Result<()> {
            let group = self.memo.canonical_group(group);
            self.reads
                .entry(group)
                .or_insert(PatternRead::from_group(self.memo, group)?);
            Ok(())
        }

        fn operator_type(&self, expression: LogicalExprId) -> Option<LogicalOperatorType> {
            let logical = self.memo.logical_expr(expression)?;
            self.state
                .metadata
                .get(&logical.payload)
                .map(|metadata| metadata.operator_type)
        }

        fn expressions_of_type(
            &mut self,
            group: GroupId,
            operator_type: LogicalOperatorType,
        ) -> Result<Vec<LogicalExprId>> {
            let group = self.memo.canonical_group(group);
            self.observe(group)?;
            Ok(self
                .memo
                .group(group)
                .ok_or_else(|| paro_error::internal("dimension-sharing probe lost group"))?
                .logical_exprs()
                .iter()
                .copied()
                .filter(|expression| self.operator_type(*expression) == Some(operator_type))
                .collect())
        }

        fn has_operator(
            &mut self,
            group: GroupId,
            operator_type: LogicalOperatorType,
        ) -> Result<bool> {
            Ok(!self.expressions_of_type(group, operator_type)?.is_empty())
        }

        fn branch_possible(&mut self, group: GroupId) -> Result<bool> {
            for projection in self.expressions_of_type(group, LogicalOperatorType::Projection)? {
                let Some(projection_logical) = self.memo.logical_expr(projection) else {
                    continue;
                };
                let [projection_child] = projection_logical.key.children.as_ref() else {
                    continue;
                };
                let mut aggregate_groups = vec![*projection_child];
                for filter in
                    self.expressions_of_type(*projection_child, LogicalOperatorType::Filter)?
                {
                    let Some(filter_logical) = self.memo.logical_expr(filter) else {
                        continue;
                    };
                    if let [aggregate_group] = filter_logical.key.children.as_ref() {
                        aggregate_groups.push(*aggregate_group);
                    }
                }
                for aggregate_group in aggregate_groups {
                    for outer in
                        self.expressions_of_type(aggregate_group, LogicalOperatorType::Aggregate)?
                    {
                        let Some(outer_logical) = self.memo.logical_expr(outer) else {
                            continue;
                        };
                        let [join_group] = outer_logical.key.children.as_ref() else {
                            continue;
                        };
                        for join in self
                            .expressions_of_type(*join_group, LogicalOperatorType::ComparisonJoin)?
                        {
                            let Some(join_logical) = self.memo.logical_expr(join) else {
                                continue;
                            };
                            let [dimension_group, partial_group] =
                                join_logical.key.children.as_ref()
                            else {
                                continue;
                            };
                            if self.has_operator(*dimension_group, LogicalOperatorType::Get)?
                                && self
                                    .has_operator(*partial_group, LogicalOperatorType::Aggregate)?
                            {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
            Ok(false)
        }

        fn union_group_possible(&mut self, group: GroupId) -> Result<bool> {
            let group = self.memo.canonical_group(group);
            self.observe(group)?;
            if !self.visiting.insert(group) {
                return Ok(false);
            }
            let expressions = self
                .memo
                .group(group)
                .ok_or_else(|| paro_error::internal("dimension-sharing probe lost group"))?
                .logical_exprs()
                .to_vec();
            for expression in expressions {
                match self.operator_type(expression) {
                    Some(LogicalOperatorType::LogicalUnion) => {
                        let Some(logical) = self.memo.logical_expr(expression) else {
                            continue;
                        };
                        let [left, right] = logical.key.children.as_ref() else {
                            continue;
                        };
                        if self.union_group_possible(*left)? && self.union_group_possible(*right)? {
                            self.visiting.remove(&group);
                            return Ok(true);
                        }
                    }
                    Some(LogicalOperatorType::Projection) if self.branch_possible(group)? => {
                        self.visiting.remove(&group);
                        return Ok(true);
                    }
                    _ => {}
                }
            }
            self.visiting.remove(&group);
            Ok(false)
        }
    }

    let root_group = memo.canonical_group(root_group);
    let root = memo
        .logical_expr(root_expression)
        .ok_or_else(|| paro_error::internal("dimension-sharing probe lost root expression"))?;
    let [left, right] = root.key.children.as_ref() else {
        return Ok(RootDispatch::default());
    };
    let mut probe = Probe {
        memo,
        state,
        reads: BTreeMap::new(),
        visiting: BTreeSet::new(),
    };
    probe.observe(root_group)?;
    let matches = probe.union_group_possible(*left)? && probe.union_group_possible(*right)?;
    Ok(RootDispatch {
        matches,
        reads: if matches {
            Box::new([])
        } else {
            probe
                .reads
                .into_values()
                .collect::<Vec<_>>()
                .into_boxed_slice()
        },
    })
}

pub(super) fn matches_transformation_root(
    transformation: PlannerTransformation,
    expr: &crate::cascades::memo::LogicalExpr,
    state: &PlannerTransformState,
) -> bool {
    let Some(metadata) = state.metadata.get(&expr.payload) else {
        return false;
    };
    if matches!(
        transformation,
        PlannerTransformation::AggregateDimensionDeferral
    ) {
        let Some(payload) = state.payloads.logical.get(expr.payload.index()) else {
            return false;
        };
        let LogicalOperator::Aggregate(aggregate) = &payload.semantic_template.operator else {
            return false;
        };
        // Only immutable root shape is negative here. Do not consult a child
        // representative or cache a failed native direct-child proof: scoped
        // matching still subscribes to the direct input frontier below.
        return crate::aggregate::dimension_deferral::root_eligible(aggregate);
    }
    if matches!(
        transformation,
        PlannerTransformation::CtePartitionedMaterialization
            | PlannerTransformation::CteInline
            | PlannerTransformation::CteDemandPushdown
            | PlannerTransformation::CteFilterPushdown
    ) {
        let Some(payload) = state.payloads.logical.get(expr.payload.index()) else {
            return false;
        };
        let LogicalOperator::MaterializedCTE(cte) = &payload.semantic_template.operator else {
            return false;
        };
        return cte_transformation_accepts(transformation, cte.materialized);
    }
    if matches!(
        transformation,
        PlannerTransformation::AggregateJoinSubsumption
    ) {
        let Some(payload) = state.payloads.logical.get(expr.payload.index()) else {
            return false;
        };
        return payload
            .scalar_facts
            .aggregate
            .as_ref()
            .is_some_and(|facts| facts.subsumable_sum_input.is_some());
    }
    if matches!(
        transformation,
        PlannerTransformation::AggregateInputMaterialization
    ) {
        let Some(payload) = state.payloads.logical.get(expr.payload.index()) else {
            return false;
        };
        return payload
            .scalar_facts
            .aggregate
            .as_ref()
            .is_some_and(|facts| !facts.materializable_inputs.is_empty());
    }
    transformation_root_operator_matches(
        transformation,
        metadata.operator_type,
        state.rowset_scan_pushdown,
    )
}

fn cte_transformation_accepts(
    transformation: PlannerTransformation,
    materialized: paro_planner::binder::ir::CTEMaterialize,
) -> bool {
    use paro_planner::binder::ir::CTEMaterialize;
    match transformation {
        PlannerTransformation::CteInline => materialized != CTEMaterialize::Materialized,
        PlannerTransformation::CtePartitionedMaterialization => {
            materialized == CTEMaterialize::Default
        }
        PlannerTransformation::CteDemandPushdown | PlannerTransformation::CteFilterPushdown => {
            materialized != CTEMaterialize::NotMaterialized
        }
        _ => false,
    }
}

/// Root dispatch is a function of the immutable operator shell and session
/// capabilities only. Equivalence provenance is deliberately absent: a rule
/// proof is audit evidence, never a semantic guard against newly added child
/// bindings of an expression produced by that same rule.
fn transformation_root_operator_matches(
    transformation: PlannerTransformation,
    operator: LogicalOperatorType,
    rowset_scan_pushdown: bool,
) -> bool {
    use LogicalOperatorType as Op;

    match transformation {
        PlannerTransformation::KeyDomainTransfer => operator == Op::ComparisonJoin,
        PlannerTransformation::PredicateTransfer => operator == Op::Filter,
        PlannerTransformation::CtePartitionedMaterialization
        | PlannerTransformation::CteInline
        | PlannerTransformation::CteDemandPushdown
        | PlannerTransformation::CteFilterPushdown => operator == Op::MaterializedCTE,
        PlannerTransformation::JoinRegionEnumeration => operator == Op::ComparisonJoin,
        PlannerTransformation::AggregatePostReduction => {
            matches!(operator, Op::MaterializedCTE | Op::Projection | Op::Filter)
        }
        // The consumer matcher only accepts a projection or filter shell. A
        // descendant publication cannot change this immutable root operator,
        // so rejecting every other root here is a complete dispatch proof and
        // avoids walking its unrelated descendant frontiers.
        PlannerTransformation::MarkJoinToSemi => matches!(operator, Op::Projection | Op::Filter),
        PlannerTransformation::JoinElimination => matches!(
            operator,
            Op::Projection | Op::Filter | Op::Aggregate | Op::Limit | Op::Order | Op::TopN
        ),
        PlannerTransformation::AggregateJoinPreaggregation
        | PlannerTransformation::AggregateJoinSubsumption
        | PlannerTransformation::AggregateNonNullInput
        | PlannerTransformation::AggregateDimensionDeferral
        | PlannerTransformation::AggregateInputMaterialization => operator == Op::Aggregate,
        PlannerTransformation::AggregateDimensionSharing => operator == Op::LogicalUnion,
        PlannerTransformation::TopNIntroduction | PlannerTransformation::LimitPushdown => {
            operator == Op::Limit
        }
        PlannerTransformation::LatePayloadFetch => {
            rowset_scan_pushdown && matches!(operator, Op::Projection | Op::TopN)
        }
        PlannerTransformation::ScalarAggregateWindow => {
            matches!(operator, Op::ComparisonJoin | Op::Projection | Op::Filter)
        }
    }
}

#[cfg(test)]
#[path = "matching_failure_tests.rs"]
mod failure_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cascades::rules::ReadScope;

    fn deferral_dispatch_input() -> OptimizationInput {
        use paro_common::types::LogicalType;
        use paro_function::aggregate::distributive::sum::get_sum_function;
        use paro_planner::expression::{AggregateExpression, ColumnRefExpression};
        use paro_planner::operator::{Aggregate, Get};
        let column = || {
            Expression::ColumnRef(
                ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::BigInt).into(),
            )
        };
        let (function, _) = get_sum_function().bind(&[LogicalType::BigInt]).unwrap();
        let ty = function.return_type.clone();
        let sum =
            Expression::Aggregate(AggregateExpression::new(function, vec![column()], ty).into());
        let plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
                1,
                2,
                3,
                OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
                    Get::new_without_table(0, vec!["k".into()], vec![LogicalType::BigInt]),
                ))),
                vec![column()],
                vec![],
                vec![sum],
                vec![],
            ))));
        MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap()
    }

    #[test]
    fn deferral_root_dispatch_shares_only_immutable_aggregate_guards() {
        use paro_common::runtime_value::Value;
        use paro_common::types::LogicalType;
        use paro_planner::binder::ir::GroupingSet;
        use paro_planner::expression::ConstantExpression;
        use paro_planner::operator::aggregate::PostAggregateReduction;

        // Mutate only the imported shell for each guard fixture: no child
        // witness or executable expression recognition is involved here.
        for (case, expected) in [
            ("plain", true),
            ("explicit_full_grouping_set", true),
            ("empty_aggregates", false),
            ("post_reduction", false),
            ("scalar_aggregate", false),
            ("empty_grouping_set", false),
            ("multiple_grouping_sets", false),
            ("duplicate_grouping_indices", false),
            ("out_of_range_grouping_index", false),
            ("grouping_function", false),
        ] {
            let input = deferral_dispatch_input();
            let expression = input
                .memo
                .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
                .unwrap();
            let mut state = input.planner_state.write().unwrap();
            let LogicalOperator::Aggregate(aggregate) = &mut state.payloads.logical
                [expression.payload.index()]
            .semantic_template
            .operator
            else {
                unreachable!()
            };
            match case {
                "empty_aggregates" => aggregate.aggregates.clear(),
                "post_reduction" => {
                    aggregate.post_reduction = Some(PostAggregateReduction {
                        reduction_index: 4,
                        reducers: vec![],
                        scalar_expressions: vec![],
                        predicate: Expression::Constant(
                            ConstantExpression::new(Value::Boolean(true), LogicalType::Boolean)
                                .into(),
                        ),
                    });
                }
                "scalar_aggregate" => aggregate.groups.clear(),
                "empty_grouping_set" => aggregate.grouping_sets = vec![GroupingSet::default()],
                "multiple_grouping_sets" => {
                    aggregate.grouping_sets = vec![
                        GroupingSet {
                            expressions: vec![0],
                        },
                        GroupingSet::default(),
                    ]
                }
                "grouping_function" => aggregate.grouping_functions = vec![vec![0]],
                "duplicate_grouping_indices" => {
                    aggregate.groups.push(aggregate.groups[0].clone());
                    aggregate.grouping_sets = vec![GroupingSet {
                        expressions: vec![0, 0],
                    }];
                }
                "out_of_range_grouping_index" => {
                    aggregate.grouping_sets = vec![GroupingSet {
                        expressions: vec![1],
                    }];
                }
                "explicit_full_grouping_set" => {
                    aggregate.grouping_sets = vec![GroupingSet {
                        expressions: vec![0],
                    }];
                }
                "plain" => {}
                _ => unreachable!(),
            }
            assert_eq!(
                crate::aggregate::dimension_deferral::root_eligible(aggregate),
                expected,
                "{case}"
            );
            assert_eq!(
                matches_transformation_root(
                    PlannerTransformation::AggregateDimensionDeferral,
                    expression,
                    &state,
                ),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn deferral_non_join_child_is_not_a_permanent_negative_root() {
        let input = deferral_dispatch_input();
        let expression = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap();
        let child = input.memo.canonical_group(expression.key.children[0]);
        let state = input.planner_state.read().unwrap();
        // The input is a Get, not a join. Root eligibility must nevertheless
        // allow discovery of later alternatives in this same input group.
        assert!(matches_transformation_root(
            PlannerTransformation::AggregateDimensionDeferral,
            expression,
            &state,
        ));
        assert!(cached_negative_root_reads(
            PlannerTransformation::AggregateDimensionDeferral,
            input.root,
            expression.id,
            &input.memo,
            &state,
        )
        .unwrap()
        .is_none());
        let bindings = scoped_pattern_bindings(
            PlannerTransformation::AggregateDimensionDeferral,
            input.root,
            expression.id,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap();
        assert!(
            bindings
                .reads
                .iter()
                .any(|read| { read.group == child && read.logical_frontier_revision.is_some() }),
            "later child publications must still invalidate discovery"
        );
    }

    #[test]
    fn aggregate_root_dispatch_uses_published_native_evidence_not_executable_payloads() {
        use paro_common::runtime_value::Value;
        use paro_common::types::LogicalType;
        use paro_function::aggregate::distributive::sum::get_sum_function;
        use paro_planner::expression::{
            AggregateExpression, ColumnRefExpression, ConstantExpression,
        };
        use paro_planner::operator::{Aggregate, Get};
        let (function, _) = get_sum_function().bind(&[LogicalType::BigInt]).unwrap();
        let ty = function.return_type.clone();
        let sum = Expression::Aggregate(
            AggregateExpression::new(
                function,
                vec![Expression::ColumnRef(
                    ColumnRefExpression::new(ColumnBinding::new(0, 0), LogicalType::BigInt).into(),
                )],
                ty,
            )
            .into(),
        );
        let plan =
            OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(Aggregate::new(
                1,
                2,
                3,
                OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
                    Get::new_without_table(0, vec!["k".into()], vec![LogicalType::BigInt]),
                ))),
                vec![],
                vec![],
                vec![sum],
                vec![],
            ))));
        let input = MemoBuilder::build(plan, BindContext::new(), SearchBudget::default()).unwrap();
        let expression = input
            .memo
            .logical_expr(input.memo.group(input.root).unwrap().logical_exprs()[0])
            .unwrap();
        let mut state = input.planner_state.write().unwrap();
        assert!(matches_transformation_root(
            PlannerTransformation::AggregateJoinSubsumption,
            expression,
            &state
        ));
        // Keep the shape/arity but poison the old executable scalar. The
        // imported native operands and their immutable evidence do not change.
        let LogicalOperator::Aggregate(aggregate) = &mut state.payloads.logical
            [expression.payload.index()]
        .semantic_template
        .operator
        else {
            unreachable!()
        };
        aggregate.aggregates[0] = Expression::Constant(
            ConstantExpression::new(Value::BigInt(99), LogicalType::BigInt).into(),
        );
        assert!(matches_transformation_root(
            PlannerTransformation::AggregateJoinSubsumption,
            expression,
            &state
        ));
    }

    #[test]
    fn detail_subsumption_keeps_unconsumed_relations_as_group_inputs() {
        use paro_common::types::LogicalType;
        use paro_planner::operator::{Aggregate, Distinct, Filter, Get};
        let mut child = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
            Get::new_without_table(0, vec!["k".into()], vec![LogicalType::BigInt]),
        )));
        for _ in 0..64 {
            child =
                OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(child, vec![])));
        }
        let child = OwnedLogicalPlan::synthetic(LogicalOperator::Distinct(Distinct::new(child)));
        let plan = OwnedLogicalPlan::synthetic(LogicalOperator::Aggregate(Box::new(
            Aggregate::new(1, 2, 3, child, vec![], vec![], vec![], vec![]),
        )));
        let mut budget = SearchBudget::default();
        budget.max_rule_work_units_per_group = 12;
        let input = MemoBuilder::build(plan, BindContext::new(), budget).unwrap();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let child = input.memo.logical_expr(expression).unwrap().key.children[0];
        let state = input.planner_state.read().unwrap();
        let bound = scoped_pattern_bindings(
            PlannerTransformation::AggregateJoinSubsumption,
            input.root,
            expression,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
        )
        .unwrap();
        assert_eq!(bound.completion, PatternEnumerationCompletion::Complete);
        assert_eq!(bound.bindings.len(), 1);
        assert_eq!(bound.reads.len(), 2);
        let PatternOperand::Expression { children, .. } = &bound.bindings[0].root else {
            panic!("aggregate shell")
        };
        assert_eq!(children.as_ref(), &[PatternOperand::Group(child)]);
    }

    #[test]
    fn local_filter_binding_does_not_enumerate_or_subscribe_below_its_input() {
        use paro_common::types::LogicalType;
        use paro_planner::operator::{Filter, Get};
        let mut child = OwnedLogicalPlan::synthetic(LogicalOperator::Get(Box::new(
            Get::new_without_table(0, vec!["k".into()], vec![LogicalType::BigInt]),
        )));
        for _ in 0..64 {
            child = OwnedLogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(
                child,
                Vec::new(),
            )));
        }
        let mut budget = SearchBudget::default();
        budget.max_rule_work_units_per_group = 8;
        let mut input = MemoBuilder::build(child, BindContext::new(), budget).unwrap();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let child = input.memo.logical_expr(expression).unwrap().key.children[0];
        let state = input.planner_state.read().unwrap();
        let before = enumerate_pattern_bindings(
            input.root,
            expression,
            &input.memo,
            input.memo.budget(),
            BudgetDimension::RuleWorkPerGroup,
            None,
            PatternSpec {
                state: Some(&state),
                scope: PatternScope::Shell,
                witness: None,
            },
        )
        .unwrap();
        assert_eq!(before.completion, PatternEnumerationCompletion::Complete);
        assert_eq!(before.bindings.len(), 1);
        assert_eq!(before.reads.len(), 2);
        assert!(before.work_units <= 8);
        let PatternOperand::Expression { children, .. } = &before.bindings[0].root else {
            panic!("bound filter")
        };
        assert_eq!(children.as_ref(), &[PatternOperand::Group(child)]);
        let payload = input
            .memo
            .logical_expr(input.memo.group(child).unwrap().logical_exprs()[0])
            .unwrap()
            .payload;
        input
            .memo
            .insert_logical(
                child,
                LogicalExprKey {
                    operator: Fingerprint(123456),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                payload,
                EquivalenceProof::Normalization {
                    rule: RuleId(123456),
                },
            )
            .unwrap();
        assert!(before
            .reads
            .iter()
            .all(|read| read.is_current(&input.memo).unwrap()));
        let after = enumerate_pattern_bindings(
            input.root,
            expression,
            &input.memo,
            input.memo.budget(),
            BudgetDimension::RuleWorkPerGroup,
            None,
            PatternSpec {
                state: Some(&state),
                scope: PatternScope::Shell,
                witness: None,
            },
        )
        .unwrap();
        assert_eq!(before.bindings, after.bindings);
    }

    fn binding_fingerprints(
        reverse: bool,
        id_shift: usize,
        proof_rule: RuleId,
    ) -> (Vec<Fingerprint>, Box<[PatternRead]>) {
        let mut memo = Memo::new(SearchBudget::default());
        let schema = GroupSchema::new(std::iter::empty()).unwrap();
        for _ in 0..id_shift {
            memo.create_group(
                schema.clone(),
                LogicalProperties::default(),
                GroupCardinality::default(),
            );
        }
        let child = memo.create_group(
            schema.clone(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let alternatives = if reverse { [20_u128, 10] } else { [10, 20] };
        for (index, operator) in alternatives.into_iter().enumerate() {
            memo.insert_logical(
                child,
                LogicalExprKey {
                    operator: Fingerprint(operator),
                    scalars: Box::new([]),
                    children: Box::new([]),
                },
                LogicalPayloadId::new(operator as usize),
                if index == 0 {
                    EquivalenceProof::Initial
                } else {
                    EquivalenceProof::Normalization { rule: proof_rule }
                },
            )
            .unwrap();
        }
        let root = memo.create_group(
            schema,
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let root_expression = memo
            .insert_logical(
                root,
                LogicalExprKey {
                    operator: Fingerprint(30),
                    scalars: Box::new([]),
                    children: Box::new([child]),
                },
                LogicalPayloadId::new(30),
                EquivalenceProof::Initial,
            )
            .unwrap();
        let bindings = pattern_bindings(
            root,
            root_expression,
            &memo,
            memo.budget(),
            BudgetDimension::RuleWorkPerGroup,
            None,
        )
        .unwrap();
        (
            bindings
                .bindings
                .iter()
                .map(|binding| binding.fingerprint)
                .collect(),
            bindings.reads,
        )
    }

    #[test]
    fn binding_closure_is_independent_of_expression_insertion_order() {
        let (forward, reads) = binding_fingerprints(false, 0, RuleId(99));
        let (reverse, _) = binding_fingerprints(true, 0, RuleId(99));
        assert_eq!(forward, reverse);
        assert_eq!(forward.len(), 2);
        assert_eq!(reads.len(), 2);
    }

    #[test]
    fn binding_closure_is_independent_of_ids_and_proof_fingerprints() {
        let (baseline, baseline_reads) = binding_fingerprints(false, 0, RuleId(99));
        let (renamed, renamed_reads) = binding_fingerprints(false, 3, RuleId(9_999));
        assert_eq!(baseline, renamed);
        assert_eq!(baseline_reads.len(), renamed_reads.len());
        assert_ne!(baseline_reads[0].group, renamed_reads[0].group);
        assert_eq!(
            baseline_reads[0].logical_frontier_revision,
            renamed_reads[0].logical_frontier_revision
        );
    }

    #[test]
    fn pattern_reads_track_facts_and_statistics_without_frontier_churn() {
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(
            GroupSchema::new(std::iter::empty()).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let initial = PatternRead::from_group(&memo, group).unwrap();
        memo.group_mut(group)
            .unwrap()
            .logical_properties
            .maximum_cardinality = Some(7);
        let with_fact = PatternRead::from_group(&memo, group).unwrap();
        assert_eq!(
            initial.logical_frontier_revision,
            with_fact.logical_frontier_revision
        );
        assert_ne!(
            initial.logical_fact_fingerprint,
            with_fact.logical_fact_fingerprint
        );

        memo.group_mut(group).unwrap().cardinality =
            GroupCardinality::new(Fingerprint(17), CardinalityRecipeKind::Statistics, 1, 4, 9);
        let with_statistics = PatternRead::from_group(&memo, group).unwrap();
        assert_eq!(
            with_fact.logical_frontier_revision,
            with_statistics.logical_frontier_revision
        );
        assert_eq!(
            with_fact.logical_fact_fingerprint,
            with_statistics.logical_fact_fingerprint
        );
        assert_ne!(
            with_fact.statistics_snapshot_fingerprint,
            with_statistics.statistics_snapshot_fingerprint
        );
    }

    #[test]
    fn structure_only_read_ignores_fact_updates_until_facts_are_observed() {
        let mut memo = Memo::new(SearchBudget::default());
        let group = memo.create_group(
            GroupSchema::new(std::iter::empty()).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let structure = PatternRead::structure_from_group(&memo, group).unwrap();
        assert_eq!(structure.scope, ReadScope::LOGICAL_FRONTIER);
        assert_eq!(structure.logical_fact_fingerprint, Fingerprint::default());
        assert_eq!(
            structure.statistics_snapshot_fingerprint,
            Fingerprint::default()
        );
        let facts_before = PatternRead::facts_from_group(&memo, group).unwrap();

        memo.update_group_facts(group, |properties, cardinality| {
            properties.maximum_cardinality = Some(11);
            *cardinality =
                GroupCardinality::new(Fingerprint(31), CardinalityRecipeKind::Statistics, 1, 2, 3);
            Ok(())
        })
        .unwrap();
        assert!(structure.is_current(&memo).unwrap());

        let merged = crate::cascades::tasks::ReadSet::new([structure, facts_before]);
        assert_eq!(merged.reads().len(), 1);
        assert_eq!(
            merged.reads()[0].scope,
            ReadScope::LOGICAL_FRONTIER.union(ReadScope::FACTS)
        );
        assert!(!merged.is_current(&memo).unwrap());
    }

    #[test]
    fn root_dispatch_has_no_equivalence_provenance_input() {
        assert!(transformation_root_operator_matches(
            PlannerTransformation::JoinRegionEnumeration,
            LogicalOperatorType::ComparisonJoin,
            true,
        ));
        assert!(transformation_root_operator_matches(
            PlannerTransformation::AggregateDimensionDeferral,
            LogicalOperatorType::Aggregate,
            true,
        ));
        assert!(!transformation_root_operator_matches(
            PlannerTransformation::JoinRegionEnumeration,
            LogicalOperatorType::Projection,
            true,
        ));
        assert!(transformation_root_operator_matches(
            PlannerTransformation::MarkJoinToSemi,
            LogicalOperatorType::Projection,
            true,
        ));
        assert!(transformation_root_operator_matches(
            PlannerTransformation::MarkJoinToSemi,
            LogicalOperatorType::Filter,
            true,
        ));
        assert!(!transformation_root_operator_matches(
            PlannerTransformation::MarkJoinToSemi,
            LogicalOperatorType::Aggregate,
            true,
        ));
    }

    #[test]
    fn cte_sql_policy_constrains_strategy_not_domain_proofs() {
        use paro_planner::binder::ir::CTEMaterialize;

        for transformation in [
            PlannerTransformation::CteDemandPushdown,
            PlannerTransformation::CteFilterPushdown,
        ] {
            assert!(cte_transformation_accepts(
                transformation,
                CTEMaterialize::Default
            ));
            assert!(cte_transformation_accepts(
                transformation,
                CTEMaterialize::Materialized
            ));
            assert!(!cte_transformation_accepts(
                transformation,
                CTEMaterialize::NotMaterialized
            ));
        }
        assert!(cte_transformation_accepts(
            PlannerTransformation::CtePartitionedMaterialization,
            CTEMaterialize::Default
        ));
        assert!(!cte_transformation_accepts(
            PlannerTransformation::CtePartitionedMaterialization,
            CTEMaterialize::Materialized
        ));
        assert!(!cte_transformation_accepts(
            PlannerTransformation::CteInline,
            CTEMaterialize::Materialized
        ));
        assert!(cte_transformation_accepts(
            PlannerTransformation::CteInline,
            CTEMaterialize::NotMaterialized
        ));
    }

    fn add_expression(
        memo: &mut Memo,
        ordinal: u128,
        children: Vec<GroupId>,
    ) -> (GroupId, LogicalExprId) {
        let group = memo.create_group(
            GroupSchema::new(std::iter::empty()).unwrap(),
            LogicalProperties::default(),
            GroupCardinality::default(),
        );
        let expression = memo
            .insert_logical(
                group,
                LogicalExprKey {
                    operator: Fingerprint(ordinal),
                    scalars: Box::new([]),
                    children: children.into_boxed_slice(),
                },
                LogicalPayloadId::new(ordinal as usize),
                EquivalenceProof::Initial,
            )
            .unwrap();
        (group, expression)
    }

    #[test]
    fn read_set_includes_consumed_root_statistics() {
        let mut memo = Memo::new(SearchBudget::default());
        let (root, expression) = add_expression(&mut memo, 9_871, vec![]);
        let bindings = pattern_bindings(
            root,
            expression,
            &memo,
            memo.budget(),
            BudgetDimension::RuleWorkPerGroup,
            None,
        )
        .unwrap();
        let root_read = bindings
            .reads
            .iter()
            .find(|read| read.group == root)
            .copied()
            .expect("root facts must be observed");
        assert_eq!(root_read.logical_frontier_revision, None);

        memo.insert_logical(
            root,
            LogicalExprKey {
                operator: Fingerprint(9_873),
                scalars: Box::new([]),
                children: Box::new([]),
            },
            LogicalPayloadId::new(9_873),
            EquivalenceProof::Normalization {
                rule: RuleId(9_873),
            },
        )
        .unwrap();
        assert!(
            root_read.is_current(&memo).unwrap(),
            "peer roots do not affect a binding anchored at one exact expression"
        );

        memo.group_mut(root).unwrap().cardinality = GroupCardinality::new(
            Fingerprint(9_872),
            CardinalityRecipeKind::Statistics,
            1,
            4,
            9,
        );
        assert!(!root_read.is_current(&memo).unwrap());
    }

    #[test]
    fn shallow_fact_cursor_does_not_hide_inherited_reads() {
        let mut memo = Memo::new(SearchBudget::default());
        let (input, _) = add_expression(&mut memo, 9001, vec![]);
        let (root, _) = add_expression(&mut memo, 9002, vec![input]);
        memo.group_mut(root).unwrap().cardinality =
            GroupCardinality::inherit(Fingerprint(1), input);
        let read = PatternRead::facts_from_group(&memo, root).unwrap();
        let local = memo.group(root).unwrap().statistics_snapshot_fingerprint();
        memo.group_mut(input).unwrap().cardinality = GroupCardinality::new(
            Fingerprint(2),
            CardinalityRecipeKind::Statistics,
            10,
            10,
            10,
        );
        assert_eq!(
            local,
            memo.group(root).unwrap().statistics_snapshot_fingerprint()
        );
        assert!(read.is_current(&memo).unwrap());
    }

    #[test]
    fn expired_optional_matching_is_limited_without_charging_work() {
        let mut budget = SearchBudget::default();
        budget.optional_time_limit = Some(std::time::Duration::ZERO);
        let mut memo = Memo::new(budget);
        let (root, expression) = add_expression(&mut memo, 7001, vec![]);
        memo.control().begin_optional();
        let bindings = pattern_bindings(
            root,
            expression,
            &memo,
            memo.budget(),
            BudgetDimension::RuleWorkPerGroup,
            None,
        )
        .unwrap();
        assert!(bindings.bindings.is_empty());
        assert!(matches!(
            bindings.completion,
            PatternEnumerationCompletion::BudgetLimited { .. }
        ));
        assert!(memo.control().deadline_reached());
    }

    #[test]
    fn work_budget_bounds_shared_dag_operand_construction() {
        fn size(operand: &PatternOperand) -> usize {
            match operand {
                PatternOperand::Group(_) => 1,
                PatternOperand::Expression { children, .. } => {
                    1 + children.iter().map(size).sum::<usize>()
                }
            }
        }

        let mut budget = SearchBudget::default();
        budget.max_rule_work_units_per_group = 8;
        let mut memo = Memo::new(budget);
        let (mut root, mut expression) = add_expression(&mut memo, 1, vec![]);
        for ordinal in 2..=9 {
            (root, expression) = add_expression(&mut memo, ordinal, vec![root, root]);
        }
        let bindings = pattern_bindings(
            root,
            expression,
            &memo,
            memo.budget(),
            BudgetDimension::RuleWorkPerGroup,
            None,
        )
        .unwrap();
        let expanded_nodes = bindings
            .bindings
            .iter()
            .map(|binding| size(&binding.root))
            .sum::<usize>();
        assert!(bindings.work_units <= 8);
        assert!(
            expanded_nodes <= 8
                || matches!(
                    bindings.completion,
                    PatternEnumerationCompletion::BudgetLimited { .. }
                )
        );
    }
}
