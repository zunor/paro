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

    impl LocalMatcher<'_> {
        fn admit_work(&mut self, units: usize) -> Result<bool> {
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

        fn expressions(&mut self, group: GroupId) -> Result<Vec<LogicalExprId>> {
            let group = self.memo.canonical_group(group);
            if !self.observe(group)? {
                return Ok(Vec::new());
            }
            let mut expressions = self
                .memo
                .group(group)
                .ok_or_else(|| {
                    paro_error::internal("local pattern matcher references an unknown group")
                })?
                .logical_exprs()
                .to_vec();
            expressions.sort_by_key(|expression| {
                self.memo
                    .logical_expr(*expression)
                    // Child group ids are allocation artifacts. The immutable
                    // operator shell contains the scalar fingerprints needed
                    // for semantic ordering; exact child choices are bound and
                    // fingerprinted separately below.
                    .map(|logical| logical.key.operator)
                    .unwrap_or_default()
            });
            Ok(expressions)
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
            Ok(self
                .expressions(group)?
                .into_iter()
                .filter(|expression| self.operator_type(*expression) == Some(operator_type))
                .collect())
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
                .map(|branch| UnionSkeleton::Branch { group, branch })
                .collect::<Vec<_>>();
            for expression in self.expressions_of_type(group, LogicalOperatorType::LogicalUnion)? {
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
            if !matcher.union_dimensions_can_share(&combined) {
                continue;
            }
            let root = match matcher.union_operand(&combined)? {
                Some(root) => root,
                None if matcher.limited => break 'frontiers,
                None => continue,
            };
            let (root, fingerprint) = root;
            // The output frontier counts semantic matches, not raw shell
            // pairs. Pre-admit the exact tree-clone work, instantiate the
            // bound candidate, and run the same proof recognizer used by
            // apply. Otherwise a finite frontier can be filled entirely by
            // structurally plausible pairs that the rule must reject.
            if !matcher.admit_work(LocalMatcher::operand_nodes(&root))? {
                break 'frontiers;
            }
            let candidate = super::super::semantic_plan::instantiate_bound_plan_with_group_holes(
                memo, state, &root, None,
            )?;
            if !crate::aggregate::dimension_sharing::recognizes_plan(&candidate.plan) {
                continue;
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
    enumerate_pattern_bindings(
        root_group,
        root_expression,
        memo,
        budget,
        work_dimension,
        cancellation,
        PatternSpec {
            state: None,
            scope: PatternScope::Subtree,
            witness: None,
        },
    )
}

/// A pattern owns only the operators whose semantics the rule reads. Opaque
/// inputs retain their group identity, facts and statistics without subscribing
/// to (or multiplying by) equivalent implementations below that boundary.
#[derive(Debug, Clone, Copy)]
enum PatternScope {
    Subtree,
    Hole,
    Shell,
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
}

impl PatternScope {
    fn children(self, operator: &LogicalOperator) -> Option<Vec<Self>> {
        let arity = operator.children().len();
        let repeat = |scope| Some(vec![scope; arity]);
        match self {
            Self::Subtree => repeat(Self::Subtree),
            Self::Hole => unreachable!("group holes do not inspect operators"),
            Self::Shell => repeat(Self::Hole),
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
                LogicalOperator::Join(join) if crate::join_order::relation_manager::RelationManager::join_is_reorderable(join)
                    || matches!(join, Join::Comparison(join) if crate::join_order::relation_manager::RelationManager::reduction_join_is_reorderable(join)) => repeat(Self::JoinRegion),
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
        PlannerTransformation::ExpensivePredicatePlacement => PatternScope::Shell,
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
        _ => PatternScope::Subtree,
    };
    let witness = match transformation {
        PlannerTransformation::AggregatePostReduction
        | PlannerTransformation::ScalarAggregateWindow => Some(PatternWitness::ScalarAggregate),
        PlannerTransformation::JoinElimination => Some(PatternWitness::OuterJoin),
        _ => None,
    };
    enumerate_pattern_bindings(
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
    )
}

#[derive(Clone, Copy)]
enum PatternWitness {
    ScalarAggregate,
    OuterJoin,
}

impl PatternWitness {
    fn matches(self, operator: &LogicalOperator) -> bool {
        match (self, operator) {
            (Self::ScalarAggregate, LogicalOperator::Aggregate(aggregate)) => {
                aggregate.groups.is_empty()
            }
            (Self::OuterJoin, LogicalOperator::Join(Join::Comparison(join))) => {
                matches!(join.join_type, JoinType::Left | JoinType::Right)
            }
            _ => false,
        }
    }
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
        fn operand_work(operand: &PatternOperand) -> usize {
            match operand {
                PatternOperand::Group(_) => 1,
                PatternOperand::Expression { children, .. } => {
                    1usize.saturating_add(children.iter().map(Self::operand_work).sum::<usize>())
                }
            }
        }

        fn admit_work(&mut self, units: usize) -> Result<bool> {
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

        fn group(
            &mut self,
            group: GroupId,
            active: &mut BTreeSet<GroupId>,
            scope: PatternScope,
        ) -> Result<Vec<(PatternOperand, Fingerprint)>> {
            let group = self.memo.canonical_group(group);
            let group_ref = self.memo.group(group).ok_or_else(|| {
                paro_error::internal("pattern matcher references an unknown Memo group")
            })?;
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
                if !self.admit_work(1)? {
                    return Ok(Vec::new());
                }
                let mut fingerprint = StableFingerprintBuilder::default();
                fingerprint.write_bytes(b"paro.pattern.group-hole.v1");
                fingerprint.write_u64(group.0 as u64);
                return Ok(vec![(PatternOperand::Group(group), fingerprint.finish())]);
            }
            let mut expressions = group_ref.logical_exprs().to_vec();
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
                if self.limited && result.len() == self.limit {
                    break;
                }
            }
            active.remove(&group);
            result.sort_by_key(|(_, fingerprint)| *fingerprint);
            result.dedup_by_key(|(_, fingerprint)| *fingerprint);
            Ok(result)
        }

        fn expression(
            &mut self,
            group: GroupId,
            expression: LogicalExprId,
            active: &mut BTreeSet<GroupId>,
            scope: PatternScope,
        ) -> Result<Vec<(PatternOperand, Fingerprint)>> {
            let logical = self.memo.logical_expr(expression).ok_or_else(|| {
                paro_error::internal("pattern matcher references an unknown logical expression")
            })?;
            let child_scopes = if matches!(scope, PatternScope::Subtree) {
                vec![scope; logical.key.children.len()]
            } else {
                let operator = &self
                    .state
                    .and_then(|state| state.payloads.logical.get(logical.payload.index()))
                    .ok_or_else(|| paro_error::internal("scoped pattern has no operator shell"))?
                    .semantic_template
                    .operator;
                if matches!(scope, PatternScope::JoinRegion)
                    && !matches!(
                        operator,
                        LogicalOperator::Get(_) | LogicalOperator::Filter(_)
                    )
                    && !matches!(operator, LogicalOperator::Join(join) if crate::join_order::relation_manager::RelationManager::join_is_reorderable(join)
                        || matches!(join, Join::Comparison(join) if crate::join_order::relation_manager::RelationManager::reduction_join_is_reorderable(join)))
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
    let can_match = match witness {
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
        PlannerTransformation::CtePartitionedMaterialization
        | PlannerTransformation::CteDemandPushdown
        | PlannerTransformation::CteFilterPushdown => materialized == CTEMaterialize::Default,
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
        PlannerTransformation::ExpensivePredicatePlacement => operator == Op::Filter,
        PlannerTransformation::CtePartitionedMaterialization
        | PlannerTransformation::CteInline
        | PlannerTransformation::CteDemandPushdown
        | PlannerTransformation::CteFilterPushdown => operator == Op::MaterializedCTE,
        PlannerTransformation::JoinRegionEnumeration => operator == Op::ComparisonJoin,
        PlannerTransformation::AggregatePostReduction => {
            matches!(operator, Op::MaterializedCTE | Op::Projection | Op::Filter)
        }
        // This transformation currently recognizes a consumed mark below a
        // transparent wrapper. Until that matcher is expressed with native
        // group holes, every known root remains a possible carrier.
        PlannerTransformation::MarkJoinToSemi => true,
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
mod tests {
    use super::*;

    #[test]
    fn detail_subsumption_keeps_unconsumed_relations_as_group_inputs() {
        use paro_common::types::LogicalType;
        use paro_planner::operator::{Aggregate, Distinct, Filter, Get};
        let mut child = LogicalPlan::synthetic(LogicalOperator::Get(Get::new_without_table(
            0,
            vec!["k".into()],
            vec![LogicalType::BigInt],
        )));
        for _ in 0..64 {
            child = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(child, vec![])));
        }
        let child = LogicalPlan::synthetic(LogicalOperator::Distinct(Distinct::new(child)));
        let plan = LogicalPlan::synthetic(LogicalOperator::Aggregate(Aggregate::new(
            1,
            2,
            3,
            child,
            vec![],
            vec![],
            vec![],
            vec![],
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
        let mut child = LogicalPlan::synthetic(LogicalOperator::Get(Get::new_without_table(
            0,
            vec!["k".into()],
            vec![LogicalType::BigInt],
        )));
        for _ in 0..64 {
            child = LogicalPlan::synthetic(LogicalOperator::Filter(Filter::new(child, Vec::new())));
        }
        let mut budget = SearchBudget::default();
        budget.max_rule_work_units_per_group = 8;
        let mut input = MemoBuilder::build(child, BindContext::new(), budget).unwrap();
        let expression = input.memo.group(input.root).unwrap().logical_exprs()[0];
        let child = input.memo.logical_expr(expression).unwrap().key.children[0];
        let state = input.planner_state.read().unwrap();
        let before = scoped_pattern_bindings(
            PlannerTransformation::ExpensivePredicatePlacement,
            input.root,
            expression,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
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
        let after = scoped_pattern_bindings(
            PlannerTransformation::ExpensivePredicatePlacement,
            input.root,
            expression,
            &input.memo,
            &state,
            None,
            BudgetDimension::RuleWorkPerGroup,
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
    }

    #[test]
    fn cte_shell_rejects_rules_after_the_sharing_choice_is_frozen() {
        use paro_planner::binder::ir::CTEMaterialize;

        for transformation in [
            PlannerTransformation::CtePartitionedMaterialization,
            PlannerTransformation::CteDemandPushdown,
            PlannerTransformation::CteFilterPushdown,
        ] {
            assert!(cte_transformation_accepts(
                transformation,
                CTEMaterialize::Default
            ));
            assert!(!cte_transformation_accepts(
                transformation,
                CTEMaterialize::Materialized
            ));
        }
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
