// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use paro_planner::logical::plan::LogicalPlanRead;

impl PhysicalPlanBuilder {
    pub(crate) fn lower_join(
        &mut self,
        join: &Join<PreparedChild>,
        join_cardinality: Option<paro_planner::logical::plan::CardinalityEstimate>,
        implementation: crate::physical::PhysicalImplementationFlavor,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        match join {
            Join::Comparison(comparison) => match implementation {
                crate::physical::PhysicalImplementationFlavor::Structural => {
                    self.lower_comparison_join(comparison, join_cardinality)
                }
                crate::physical::PhysicalImplementationFlavor::HashJoin
                | crate::physical::PhysicalImplementationFlavor::HashJoinRuntimeFilter => {
                    if !comparison.duplicate_eliminated_columns.is_empty()
                        || comparison.delim_flipped
                        || !comparison.build_side_constraint.allows_right()
                        || !supports_typed_hash_join_type(comparison.join_type)
                        || !comparison
                            .conditions
                            .iter()
                            .any(|condition| is_hash_join_comparison(condition.comparison))
                        || !crate::physical::hash_join_mark_contract_is_supported(
                            comparison.join_type,
                            comparison.mark_semantics,
                            comparison
                                .conditions
                                .iter()
                                .any(|condition| !is_hash_join_comparison(condition.comparison)),
                        )
                    {
                        return Err(paro_error::internal(
                            "Physical selection chose hash join for an ineligible comparison join",
                        ));
                    }
                    self.lower_comparison_hash_join(comparison)
                }
                crate::physical::PhysicalImplementationFlavor::HashJoinBuildLeft
                | crate::physical::PhysicalImplementationFlavor::HashJoinBuildLeftRuntimeFilter => {
                    if !comparison.duplicate_eliminated_columns.is_empty()
                        || comparison.delim_flipped
                        || !comparison.build_side_constraint.allows_left()
                        || comparison.anti_join_mode != AntiJoinMode::Regular
                        || !matches!(
                            comparison.join_type,
                            JoinType::Left
                                | JoinType::Right
                                | JoinType::Inner
                                | JoinType::Outer
                                | JoinType::Semi
                                | JoinType::Anti
                                | JoinType::RightSemi
                                | JoinType::RightAnti
                        )
                        || !comparison
                            .conditions
                            .iter()
                            .any(|condition| is_hash_join_comparison(condition.comparison))
                    {
                        return Err(paro_error::internal(
                            "Physical selection chose build-left hash join for an ineligible comparison join",
                        ));
                    }
                    self.lower_comparison_hash_join_build_left(comparison)
                }
                crate::physical::PhysicalImplementationFlavor::NestedLoopJoin => {
                    if comparison.anti_join_mode == AntiJoinMode::NullAware
                        || !comparison.duplicate_eliminated_columns.is_empty()
                        || comparison.delim_flipped
                    {
                        return Err(paro_error::internal(
                            "Physical selection chose nested-loop join for an ineligible comparison join",
                        ));
                    }
                    self.lower_nested_loop_join(comparison)
                }
                crate::physical::PhysicalImplementationFlavor::SortRangeJoin => {
                    if !is_sort_range_join_candidate(comparison, join_cardinality) {
                        return Err(paro_error::internal(
                            "Physical selection chose sort-range join for an ineligible comparison join",
                        ));
                    }
                    self.lower_sort_range_join(comparison)
                }
                crate::physical::PhysicalImplementationFlavor::ClassicIeJoin => {
                    if !is_classic_ie_join_candidate(comparison, join_cardinality) {
                        return Err(paro_error::internal(
                            "Physical selection chose classic IE join for an ineligible comparison join",
                        ));
                    }
                    self.lower_classic_ie_join(comparison)
                }
                _ => Err(paro_error::internal(
                    "Physical selection chose a non-join implementation for ComparisonJoin",
                )),
            },
            Join::Any(any) => {
                if !matches!(
                    implementation,
                    crate::physical::PhysicalImplementationFlavor::Structural
                        | crate::physical::PhysicalImplementationFlavor::NestedLoopJoin
                ) || !any.build_side_constraint.allows_right()
                {
                    return Err(paro_error::internal(
                        "Physical selection chose an incompatible implementation for AnyJoin",
                    ));
                }
                self.lower_any_join(any)
            }
            Join::Cross(cross) => {
                if !cross.build_side_constraint.allows_right() {
                    return Err(paro_error::internal(
                        "cross product cannot materialize its constrained probe input",
                    ));
                }
                let spill_policy = match implementation {
                    // Structural lowering is limited to test/utility plans
                    // that bypasses relational region planning. It receives the
                    // canonical non-adaptive representation.
                    crate::physical::PhysicalImplementationFlavor::Structural
                    | crate::physical::PhysicalImplementationFlavor::CrossProductInMemory => {
                        SpillExecutionPolicy::InMemory
                    }
                    crate::physical::PhysicalImplementationFlavor::CrossProductExternal => {
                        SpillExecutionPolicy::ForcedExternal
                    }
                    _ => {
                        return Err(paro_error::internal(
                            "Physical selection chose an incompatible implementation for CrossProduct",
                        ));
                    }
                };
                self.lower_cross_product(cross, spill_policy)
            }
        }
    }

    pub(crate) fn lower_comparison_join(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
        join_cardinality: Option<paro_planner::logical::plan::CardinalityEstimate>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if join.anti_join_mode == AntiJoinMode::NullAware
            && (join.join_type != JoinType::Anti
                || join.conditions.len() != 1
                || join.conditions[0].comparison != JoinComparisonType::Equal)
        {
            return Err(paro_error::internal(
                "null-aware anti join requires one ordinary equality condition",
            ));
        }
        if !supports_typed_hash_join_type(join.join_type) {
            return self.reject_unimplemented(
                "JOIN",
                format!("typed join does not support {} join", join.join_type),
            );
        }
        if !join.duplicate_eliminated_columns.is_empty() || join.delim_flipped {
            return self.lower_comparison_delim_join(join);
        }
        if let Some(cascade) = self.try_lower_reduction_cascade(join)? {
            return Ok(cascade);
        }
        let has_hash_key = join
            .conditions
            .iter()
            .any(|condition| is_hash_join_comparison(condition.comparison));
        let mark_has_residual = join.join_type == JoinType::Mark
            && join
                .conditions
                .iter()
                .any(|condition| !is_hash_join_comparison(condition.comparison));
        if has_hash_key && !mark_has_residual {
            return self.lower_comparison_hash_join(join);
        }
        if join.anti_join_mode == AntiJoinMode::NullAware {
            return Err(paro_error::internal(
                "null-aware anti join requires hashable equality conditions",
            ));
        }
        if is_classic_ie_join_candidate(join, join_cardinality) {
            return self.lower_classic_ie_join(join);
        }
        if is_sort_range_join_candidate(join, join_cardinality) {
            return self.lower_sort_range_join(join);
        }
        self.lower_nested_loop_join(join)
    }

    pub(crate) fn lower_nested_loop_join(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.extract_node(join.left.as_ref())?;
        let right = self.extract_node(join.right.as_ref())?;
        let left_projection = nlj_left_projection(join);
        let right_projection = nlj_right_projection(join);
        let left_names =
            project_output_names(join.left.as_ref(), &left_projection, "nlj left output")?;
        let left_types = project_by_index(&join.left.types(), &left_projection, "nlj left")?;
        let right_names =
            project_output_names(join.right.as_ref(), &right_projection, "nlj right output")?;
        let right_types = project_by_index(&join.right.types(), &right_projection, "nlj right")?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let spec = NestedLoopJoinSpec {
            join_type: join.join_type,
            conditions: join.conditions.clone().into_boxed_slice(),
            mark_semantics: join.mark_semantics,
            arbitrary_condition: None,
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((
            PhysicalNodeKind::NestedLoopJoin(Box::new(spec)),
            vec![left, right],
        ))
    }

    pub(crate) fn lower_sort_range_join(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.extract_node(join.left.as_ref())?;
        let right = self.extract_node(join.right.as_ref())?;
        let left_projection = nlj_left_projection(join);
        let right_projection = nlj_right_projection(join);
        let left_names = project_output_names(
            join.left.as_ref(),
            &left_projection,
            "sort-range join left output",
        )?;
        let left_types =
            project_by_index(&join.left.types(), &left_projection, "sort-range join left")?;
        let right_names = project_output_names(
            join.right.as_ref(),
            &right_projection,
            "sort-range join right output",
        )?;
        let right_types = project_by_index(
            &join.right.types(),
            &right_projection,
            "sort-range join right",
        )?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let spec = SortRangeJoinSpec {
            join_type: join.join_type,
            conditions: join.conditions.clone().into_boxed_slice(),
            mark_semantics: join.mark_semantics,
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::SortRangeJoin(spec), vec![left, right]))
    }

    pub(crate) fn lower_classic_ie_join(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.extract_node(join.left.as_ref())?;
        let right = self.extract_node(join.right.as_ref())?;
        let left_projection = nlj_left_projection(join);
        let right_projection = nlj_right_projection(join);
        let left_names = project_output_names(
            join.left.as_ref(),
            &left_projection,
            "classic IE join left output",
        )?;
        let left_types =
            project_by_index(&join.left.types(), &left_projection, "classic IE join left")?;
        let right_names = project_output_names(
            join.right.as_ref(),
            &right_projection,
            "classic IE join right output",
        )?;
        let right_types = project_by_index(
            &join.right.types(),
            &right_projection,
            "classic IE join right",
        )?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let spec = ClassicIeJoinSpec {
            join_type: join.join_type,
            conditions: join.conditions.clone().into_boxed_slice(),
            mark_semantics: join.mark_semantics,
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::ClassicIeJoin(spec), vec![left, right]))
    }

    pub(crate) fn lower_any_join(
        &mut self,
        any: &paro_planner::logical::operator::join::AnyJoin<PreparedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if !supports_typed_hash_join_type(any.join_type) {
            return self.reject_unimplemented(
                "JOIN",
                format!("typed join does not support {} join", any.join_type),
            );
        }
        let left = self.extract_node(any.left.as_ref())?;
        let right = self.extract_node(any.right.as_ref())?;
        let left_projection = any.left_projection_map.to_indices(any.left.types().len());
        let right_projection = any.right_projection_map.to_indices(any.right.types().len());
        let left_names =
            project_output_names(any.left.as_ref(), &left_projection, "any join left")?;
        let left_types = project_by_index(&any.left.types(), &left_projection, "any join left")?;
        let right_names =
            project_output_names(any.right.as_ref(), &right_projection, "any join right")?;
        let right_types =
            project_by_index(&any.right.types(), &right_projection, "any join right")?;
        let output_names = join_output_names(any.join_type, left_names, right_names);
        let output_types = any.get_types();
        let spec = NestedLoopJoinSpec {
            join_type: any.join_type,
            conditions: Box::new([]),
            mark_semantics: MarkJoinSemantics::for_join_type(any.join_type),
            arbitrary_condition: Some(any.condition.clone()),
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((
            PhysicalNodeKind::NestedLoopJoin(Box::new(spec)),
            vec![left, right],
        ))
    }

    pub(crate) fn lower_cross_product(
        &mut self,
        cross: &CrossProduct<PreparedChild>,
        spill_policy: SpillExecutionPolicy,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.extract_node(cross.left.as_ref())?;
        let right = self.extract_node(cross.right.as_ref())?;
        let mut output_names = align_output_names(
            cross.left.output_names(),
            cross.left.types().len(),
            "cross product left output",
        )?;
        output_names.extend(align_output_names(
            cross.right.output_names(),
            cross.right.types().len(),
            "cross product right output",
        )?);
        let output_types = cross.get_types();
        let spec = CrossProductSpec {
            left_output_types: cross.left.types().into_boxed_slice(),
            right_output_types: cross.right.types().into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
            spill_policy,
        };
        Ok((PhysicalNodeKind::CrossProduct(spec), vec![left, right]))
    }

    pub(crate) fn lower_comparison_hash_join(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.extract_node(join.left.as_ref())?;
        let right = self.extract_node(join.right.as_ref())?;
        let left_projection = hash_join_left_projection(join);
        let right_projection = hash_join_right_projection(join);
        let left_names = project_output_names(
            join.left.as_ref(),
            &left_projection,
            "hash join left output",
        )?;
        let left_types = project_by_index(&join.left.types(), &left_projection, "hash join left")?;
        let right_names = project_output_names(
            join.right.as_ref(),
            &right_projection,
            "hash join right output",
        )?;
        let right_types =
            project_by_index(&join.right.types(), &right_projection, "hash join right")?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let output_permutation = OutputPermutation::identity(output_types.len());
        let (key_conditions, residual_conditions) = partition_hash_join_conditions(join);
        let build_keys_unique =
            hash_join_build_keys_are_declared_unique(join.right.as_ref(), &key_conditions);
        let build_time_integer_index =
            plan_build_time_integer_join_index(join.right.as_ref(), &key_conditions);
        let probe_residual_count = residual_conditions.len();
        let build_output_count = right_types.len();
        let mut build_payload_types = right_types;
        build_payload_types.extend(
            residual_conditions
                .iter()
                .map(|condition| condition.right.return_type()),
        );
        let spec = HashJoinSpec {
            join_type: join.join_type,
            anti_join_mode: join.anti_join_mode,
            mark_semantics: join.mark_semantics,
            build_keys_unique,
            build_time_integer_index,
            key_conditions,
            build_residual_conditions: residual_conditions,
            probe_residual_count,
            left_projection: left_projection.into_boxed_slice(),
            build_input_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            build_output_count,
            build_payload_types: build_payload_types.into_boxed_slice(),
            output_permutation,
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
            spill_policy: self
                .ctx
                .spill_execution_policy(supports_external_hash_join_type(join.join_type)),
            runtime_filter: None,
            reduction_cascade: None,
        };
        Ok((PhysicalNodeKind::HashJoin(spec), vec![left, right]))
    }

    /// Lower a hash join whose logical left input is the physical build side.
    ///
    /// The immutable hash-join executor always receives probe then build. The
    /// join is therefore inverted here. `output_permutation` maps its natural
    /// `[logical right, logical left]` layout directly into the logical
    /// `[logical left, logical right]` contract during result construction.
    pub(crate) fn lower_comparison_hash_join_build_left(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let probe = self.extract_node(join.right.as_ref())?;
        let build = self.extract_node(join.left.as_ref())?;
        let probe_projection = hash_join_right_projection(join);
        let build_projection = hash_join_left_projection(join);
        let probe_names = project_output_names(
            join.right.as_ref(),
            &probe_projection,
            "build-left hash join probe output",
        )?;
        let probe_types = project_by_index(
            &join.right.types(),
            &probe_projection,
            "build-left hash join probe",
        )?;
        let build_names = project_output_names(
            join.left.as_ref(),
            &build_projection,
            "build-left hash join build output",
        )?;
        let build_types = project_by_index(
            &join.left.types(),
            &build_projection,
            "build-left hash join build",
        )?;
        let physical_join_type = join.join_type.inverse().ok_or_else(|| {
            paro_error::internal("build-left hash join has no inverse join semantics")
        })?;
        let output_names = join_output_names(join.join_type, build_names, probe_names);
        let output_types = join.get_types();
        let probe_output_count = probe_types.len();
        let build_output_count = build_types.len();
        let output_permutation = OutputPermutation::from_forward(
            (0..probe_output_count)
                .map(|probe_index| build_output_count + probe_index)
                .chain(0..build_output_count),
        )?;
        let swapped_conditions = join
            .conditions
            .iter()
            .map(|condition| JoinCondition {
                left: condition.right.clone(),
                right: condition.left.clone(),
                comparison: condition.comparison.flip(),
            })
            .collect::<Vec<_>>();
        let (key_conditions, residual_conditions): (Vec<_>, Vec<_>) = swapped_conditions
            .into_iter()
            .partition(|condition| is_hash_join_comparison(condition.comparison));
        debug_assert!(
            !key_conditions.is_empty(),
            "hash join requires an equality key"
        );
        let key_conditions = key_conditions.into_boxed_slice();
        let residual_conditions = residual_conditions.into_boxed_slice();
        let build_keys_unique =
            hash_join_build_keys_are_declared_unique(join.left.as_ref(), &key_conditions);
        let build_time_integer_index =
            plan_build_time_integer_join_index(join.left.as_ref(), &key_conditions);
        let probe_residual_count = residual_conditions.len();
        let mut build_payload_types = build_types;
        build_payload_types.extend(
            residual_conditions
                .iter()
                .map(|condition| condition.right.return_type()),
        );
        let spec = HashJoinSpec {
            join_type: physical_join_type,
            anti_join_mode: join.anti_join_mode,
            mark_semantics: join.mark_semantics,
            build_keys_unique,
            build_time_integer_index,
            key_conditions,
            build_residual_conditions: residual_conditions,
            probe_residual_count,
            left_projection: probe_projection.into_boxed_slice(),
            build_input_projection: build_projection.into_boxed_slice(),
            left_output_types: probe_types.into_boxed_slice(),
            build_output_count,
            build_payload_types: build_payload_types.into_boxed_slice(),
            output_permutation,
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
            spill_policy: self
                .ctx
                .spill_execution_policy(supports_external_hash_join_type(physical_join_type)),
            runtime_filter: None,
            reduction_cascade: None,
        };
        Ok((PhysicalNodeKind::HashJoin(spec), vec![probe, build]))
    }

    /// Fuse consecutive build-preserving existential reductions that read the
    /// same base relation. The filtering aliases are semantically independent,
    /// but their scans need not be: one candidate stream can classify every
    /// reduction while the preserved relation is stored once.
    fn try_lower_reduction_cascade(
        &mut self,
        root: &ComparisonJoin<PreparedChild>,
    ) -> Result<Option<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)>> {
        let mut joins = Vec::new();
        let mut current = root;
        loop {
            if !matches!(current.join_type, JoinType::RightSemi | JoinType::RightAnti)
                || current.anti_join_mode != AntiJoinMode::Regular
                || !current.duplicate_eliminated_columns.is_empty()
                || current.delim_flipped
                || !current.left_projection_map.is_none()
                || current
                    .conditions
                    .iter()
                    .any(|condition| !reduction_condition_can_share_evaluation(condition))
            {
                return Ok(None);
            }
            // The outer reduction may project the preserved row down to the
            // columns consumed above the cascade. Inner reductions still feed
            // another join condition and therefore must retain their complete
            // preserved input layout.
            if !joins.is_empty()
                && !current
                    .right_projection_map
                    .is_identity(current.right.types().len())
            {
                return Ok(None);
            }
            joins.push(current);
            let LogicalOperator::Join(Join::Comparison(child)) = &current.right.operator else {
                break;
            };
            if !matches!(child.join_type, JoinType::RightSemi | JoinType::RightAnti) {
                break;
            }
            current = child;
        }
        if joins.len() < 2 || joins.len() > u8::BITS as usize {
            return Ok(None);
        }

        let preserved = current.right.as_ref();
        if joins
            .iter()
            .any(|join| join.right.types() != preserved.types())
        {
            return Ok(None);
        }

        let Some(branches) = joins
            .iter()
            .map(|join| ReductionScanBranch::inspect(join.left.as_ref()))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let Some(first_table) = branches
            .first()
            .and_then(|branch| branch.get.table.as_ref())
        else {
            return Ok(None);
        };
        if branches.iter().any(|branch| {
            branch
                .get
                .table
                .as_ref()
                .is_none_or(|table| !Arc::ptr_eq(table, first_table))
                || branch
                    .filters
                    .iter()
                    .chain(branch.get.runtime_filter_expressions.iter())
                    .any(|expression| !expression.evaluation_properties().can_share_evaluation())
        }) {
            return Ok(None);
        }

        let mut merged_column_ids = Vec::new();
        for branch in &branches {
            for column_id in branch.filter_column_ids.iter().copied() {
                if !merged_column_ids.contains(&column_id) {
                    merged_column_ids.push(column_id);
                }
            }
        }
        let mut merged_get = branches[0].get.clone();
        merged_get.column_sources = merged_column_ids
            .iter()
            .copied()
            .map(|column_id| paro_planner::logical::operator::GetColumnSource::Stored { column_id })
            .collect();
        merged_get.names = merged_column_ids
            .iter()
            .map(|&column_id| first_table.columns[column_id].name.clone())
            .collect();
        merged_get.column_types = merged_column_ids
            .iter()
            .map(|&column_id| first_table.columns[column_id].logical_type.clone())
            .collect();
        merged_get.returned_types = merged_get.column_types.clone();
        if branches.iter().skip(1).any(|branch| {
            !scan_orders_are_fusion_compatible(
                branches[0].get.scan_order.as_ref(),
                branch.get.scan_order.as_ref(),
            )
        }) {
            return Ok(None);
        }
        let mut branch_runtime_filters = Vec::with_capacity(branches.len());
        for branch in &branches {
            let remapped = branch
                .get
                .runtime_filter_expressions
                .iter()
                .map(|expression| {
                    remap_reduction_expression(
                        expression,
                        branch.filter_column_ids(),
                        branch.get.table_index,
                        &merged_column_ids,
                        merged_get.table_index,
                    )
                })
                .collect::<Option<Vec<_>>>();
            branch_runtime_filters.push(remapped);
        }
        let shared_scan_width = merged_get
            .column_types
            .iter()
            .map(|ty| self.ctx.scan_access_cost.estimated_width(ty))
            .sum();
        let independent_scan_width = branches
            .iter()
            .flat_map(|branch| branch.get.column_types.iter())
            .map(|ty| self.ctx.scan_access_cost.estimated_width(ty))
            .sum();
        let Some(runtime_filters) = plan_reduction_runtime_filter_fusion(
            branch_runtime_filters,
            shared_scan_width,
            independent_scan_width,
        ) else {
            return Ok(None);
        };
        merged_get.runtime_filter_expressions = runtime_filters;

        let mut common_keys: Option<Vec<JoinCondition>> = None;
        let mut reduction_steps = Vec::with_capacity(joins.len());
        let mut reduction_predicates: Vec<HashReductionPredicateSpec> = Vec::new();
        let mut build_residual_conditions: Vec<JoinCondition> = Vec::new();
        let mut reduction_source_predicates: Vec<HashReductionSourcePredicateSpec> = Vec::new();
        let mut required_mask = 0u8;
        let mut forbidden_mask = 0u8;
        let mut predicate_bits = ReductionPredicateBits::default();

        for (step_idx, (join, branch)) in joins.iter().zip(&branches).enumerate() {
            let mut keys = Vec::new();
            let mut residuals = Vec::new();
            for condition in &join.conditions {
                let Some(left) = remap_reduction_expression(
                    &condition.left,
                    branch.condition_column_ids(),
                    branch.get.table_index,
                    &merged_column_ids,
                    merged_get.table_index,
                ) else {
                    return Ok(None);
                };
                let Some(left) = bind_reduction_source_expression(left, merged_get.table_index)
                else {
                    return Ok(None);
                };
                let condition =
                    JoinCondition::new(left, condition.right.clone(), condition.comparison);
                if is_hash_join_comparison(condition.comparison) {
                    keys.push(condition);
                } else {
                    residuals.push(condition);
                }
            }
            if keys.is_empty() {
                return Ok(None);
            }
            match &common_keys {
                None => common_keys = Some(keys),
                Some(common) if same_reduction_keys(common, &keys) => {}
                Some(_) => return Ok(None),
            }

            let match_mask = 1u8 << step_idx;
            match join.join_type {
                JoinType::RightSemi => required_mask |= match_mask,
                JoinType::RightAnti => forbidden_mask |= match_mask,
                _ => return Ok(None),
            }
            let mut predicate_mask = 0u8;
            for residual in residuals {
                let predicate_bit = if let Some(existing) =
                    reduction_predicates.iter().find(|existing| {
                        same_reduction_condition(
                            &build_residual_conditions[existing.build_residual_offset],
                            &residual,
                        )
                    }) {
                    existing.predicate_mask
                } else {
                    let Some(predicate_bit) = predicate_bits.allocate() else {
                        return Ok(None);
                    };
                    let build_residual_offset = build_residual_conditions.len();
                    build_residual_conditions.push(residual);
                    reduction_predicates.push(HashReductionPredicateSpec {
                        build_residual_offset,
                        predicate_mask: predicate_bit,
                    });
                    predicate_bit
                };
                predicate_mask |= predicate_bit;
            }
            for filter in &branch.filters {
                let Some(expression) = remap_reduction_expression(
                    filter,
                    branch.filter_column_ids(),
                    branch.get.table_index,
                    &merged_column_ids,
                    merged_get.table_index,
                ) else {
                    return Ok(None);
                };
                let Some(expression) =
                    bind_reduction_source_expression(expression, merged_get.table_index)
                else {
                    return Ok(None);
                };
                let source_mask = if let Some(existing) = reduction_source_predicates
                    .iter()
                    .find(|existing| existing.expression.equals(&expression))
                {
                    existing.predicate_mask
                } else {
                    let Some(source_mask) = predicate_bits.allocate() else {
                        return Ok(None);
                    };
                    reduction_source_predicates.push(HashReductionSourcePredicateSpec {
                        expression,
                        predicate_mask: source_mask,
                    });
                    source_mask
                };
                predicate_mask |= source_mask;
            }
            reduction_steps.push(HashReductionStepSpec {
                predicate_mask,
                match_mask,
            });
        }

        let Some(key_conditions) = common_keys else {
            return Ok(None);
        };
        let grouped_extrema = plan_grouped_extrema_reduction(
            &key_conditions,
            &build_residual_conditions,
            &reduction_predicates,
            &reduction_source_predicates,
            &reduction_steps,
        );
        let (source_kind, source_children) = self.lower_get(&merged_get)?;
        let source_output =
            RowType::new(merged_get.names.clone(), merged_get.returned_types.clone());
        let source = self.push_node(
            source_kind,
            source_output,
            source_children,
            OperatorLabel::new(joins[0].left.id, "ROWSET_SCAN"),
            None,
        );
        let build = self.extract_node(preserved)?;
        let preserved_types = preserved.types();
        let build_input_projection = root.right_projection_map.to_indices(preserved_types.len());
        let mut build_payload_types = project_by_index(
            &preserved_types,
            &build_input_projection,
            "reduction cascade preserved output",
        )?;
        let build_output_count = build_payload_types.len();
        build_payload_types.extend(
            build_residual_conditions
                .iter()
                .map(|condition| condition.right.return_type()),
        );
        let spec = HashJoinSpec {
            join_type: JoinType::RightSemi,
            anti_join_mode: AntiJoinMode::Regular,
            mark_semantics: MarkJoinSemantics::NotMark,
            build_keys_unique: false,
            build_time_integer_index: None,
            key_conditions: key_conditions.into_boxed_slice(),
            build_residual_conditions: build_residual_conditions.into_boxed_slice(),
            probe_residual_count: 0,
            left_projection: Box::new([]),
            build_input_projection: build_input_projection.into_boxed_slice(),
            left_output_types: Box::new([]),
            build_output_count,
            build_payload_types: build_payload_types.into_boxed_slice(),
            output_permutation: OutputPermutation::identity(root.get_types().len()),
            output_names: comparison_join_output_names(root)?.into_boxed_slice(),
            output_types: root.get_types().into_boxed_slice(),
            spill_policy: self.ctx.spill_execution_policy(true),
            runtime_filter: None,
            reduction_cascade: Some(HashReductionCascadeSpec {
                predicates: reduction_predicates.into_boxed_slice(),
                source_predicates: reduction_source_predicates.into_boxed_slice(),
                steps: reduction_steps.into_boxed_slice(),
                required_mask,
                forbidden_mask,
                grouped_extrema,
            }),
        };
        Ok(Some((
            PhysicalNodeKind::HashJoin(spec),
            vec![source, build],
        )))
    }

    pub(crate) fn lower_comparison_delim_join(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if join.duplicate_eliminated_columns.is_empty() {
            return self.reject_unimplemented(
                "JOIN",
                "delim-flipped comparison join is missing duplicate-eliminated keys",
            );
        }
        let (side, capture_input, wrapped_non_cached) = if join.delim_flipped {
            (
                DelimJoinSideSpec::Right,
                self.extract_node(join.right.as_ref())?,
                self.extract_node(join.left.as_ref())?,
            )
        } else {
            (
                DelimJoinSideSpec::Left,
                self.extract_node(join.left.as_ref())?,
                self.extract_node(join.right.as_ref())?,
            )
        };
        let cached_outer_output = self.plan_node_output(capture_input).clone();
        let cached_outer = self.synthetic_cached_outer_scan(
            cached_outer_output.clone(),
            match side {
                DelimJoinSideSpec::Left => join.left.id,
                DelimJoinSideSpec::Right => join.right.id,
            },
        );
        let (wrapped_left, wrapped_right) = match side {
            DelimJoinSideSpec::Left => (cached_outer, wrapped_non_cached),
            DelimJoinSideSpec::Right => (wrapped_non_cached, cached_outer),
        };

        let has_hash_key = join
            .conditions
            .iter()
            .any(|condition| is_hash_join_comparison(condition.comparison));
        let has_non_hash_condition = join
            .conditions
            .iter()
            .any(|condition| !is_hash_join_comparison(condition.comparison));
        let mark_contract_supported = crate::physical::hash_join_mark_contract_is_supported(
            join.join_type,
            join.mark_semantics,
            has_non_hash_condition,
        );
        let wrapped_join = if has_hash_key && mark_contract_supported {
            self.push_wrapped_hash_join(join, wrapped_left, wrapped_right)?
        } else {
            self.push_wrapped_nlj(join, wrapped_left, wrapped_right)?
        };
        debug_assert_eq!(
            self.plan_node_output(wrapped_join).types.as_ref(),
            join.get_types(),
            "wrapped delim join output must match logical join output"
        );
        debug_assert_eq!(
            self.plan_node_output(match side {
                DelimJoinSideSpec::Left => wrapped_left,
                DelimJoinSideSpec::Right => wrapped_right,
            }),
            cached_outer_output,
            "cached outer scan must preserve captured input row type"
        );

        let spec = DelimJoinSpec {
            side,
            duplicate_keys: join.duplicate_eliminated_columns.clone().into_boxed_slice(),
            output_names: comparison_join_output_names(join)?.into_boxed_slice(),
            output_types: join.get_types().into_boxed_slice(),
        };
        Ok((
            PhysicalNodeKind::DelimJoin(spec),
            vec![capture_input, wrapped_join],
        ))
    }

    pub(crate) fn synthetic_cached_outer_scan(
        &mut self,
        output: RowType,
        logical_id: paro_planner::logical::plan::PlanNodeId,
    ) -> PhysicalPlanNodeId {
        let spec = DelimScanSpec {
            target: DelimScanTarget::CachedOuter,
            output_names: output.names.clone(),
            output_types: output.types.clone(),
        };
        self.push_node(
            PhysicalNodeKind::DelimScan(spec),
            output,
            Vec::new(),
            OperatorLabel::new(logical_id, "DELIM_CACHED_OUTER"),
            None,
        )
    }

    pub(crate) fn push_wrapped_hash_join(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
        left: PhysicalPlanNodeId,
        right: PhysicalPlanNodeId,
    ) -> Result<PhysicalPlanNodeId> {
        let left_projection = hash_join_left_projection(join);
        let right_projection = hash_join_right_projection(join);
        let left_names = project_output_names(
            join.left.as_ref(),
            &left_projection,
            "delim hash join left output",
        )?;
        let left_types =
            project_by_index(&join.left.types(), &left_projection, "delim hash join left")?;
        let right_names = project_output_names(
            join.right.as_ref(),
            &right_projection,
            "delim hash join right output",
        )?;
        let right_types = project_by_index(
            &join.right.types(),
            &right_projection,
            "delim hash join right",
        )?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let output_permutation = OutputPermutation::identity(output_types.len());
        let (key_conditions, residual_conditions) = partition_hash_join_conditions(join);
        let build_keys_unique =
            hash_join_build_keys_are_declared_unique(join.right.as_ref(), &key_conditions);
        let build_time_integer_index =
            plan_build_time_integer_join_index(join.right.as_ref(), &key_conditions);
        let probe_residual_count = residual_conditions.len();
        let build_output_count = right_types.len();
        let mut build_payload_types = right_types;
        build_payload_types.extend(
            residual_conditions
                .iter()
                .map(|condition| condition.right.return_type()),
        );
        let spec = HashJoinSpec {
            join_type: join.join_type,
            anti_join_mode: join.anti_join_mode,
            mark_semantics: join.mark_semantics,
            build_keys_unique,
            build_time_integer_index,
            key_conditions,
            build_residual_conditions: residual_conditions,
            probe_residual_count,
            left_projection: left_projection.into_boxed_slice(),
            build_input_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            build_output_count,
            build_payload_types: build_payload_types.into_boxed_slice(),
            output_permutation,
            output_names: output_names.clone().into_boxed_slice(),
            output_types: output_types.clone().into_boxed_slice(),
            spill_policy: self
                .ctx
                .spill_execution_policy(supports_external_hash_join_type(join.join_type)),
            runtime_filter: None,
            reduction_cascade: None,
        };
        let mut output_identities = spec
            .left_projection
            .iter()
            .filter_map(|index| self.plan_node_output(left).identities.get(*index).cloned())
            .collect::<Vec<_>>();
        output_identities.extend(
            spec.build_input_projection
                .iter()
                .filter_map(|index| self.plan_node_output(right).identities.get(*index).cloned())
                .take(build_output_count),
        );
        output_identities.resize(output_types.len(), ColumnIdentity::Internal);
        Ok(self.push_node(
            PhysicalNodeKind::HashJoin(spec),
            RowType::with_identities(output_names, output_types, output_identities),
            vec![left, right],
            OperatorLabel::new(join.left.id, "HASH_JOIN"),
            None,
        ))
    }

    pub(crate) fn push_wrapped_nlj(
        &mut self,
        join: &ComparisonJoin<PreparedChild>,
        left: PhysicalPlanNodeId,
        right: PhysicalPlanNodeId,
    ) -> Result<PhysicalPlanNodeId> {
        let left_projection = nlj_left_projection(join);
        let right_projection = nlj_right_projection(join);
        let left_names = project_output_names(
            join.left.as_ref(),
            &left_projection,
            "delim nlj left output",
        )?;
        let left_types = project_by_index(&join.left.types(), &left_projection, "delim nlj left")?;
        let right_names = project_output_names(
            join.right.as_ref(),
            &right_projection,
            "delim nlj right output",
        )?;
        let right_types =
            project_by_index(&join.right.types(), &right_projection, "delim nlj right")?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let spec = NestedLoopJoinSpec {
            join_type: join.join_type,
            conditions: join.conditions.clone().into_boxed_slice(),
            mark_semantics: join.mark_semantics,
            arbitrary_condition: None,
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.clone().into_boxed_slice(),
            output_types: output_types.clone().into_boxed_slice(),
        };
        let mut output_identities = spec
            .left_projection
            .iter()
            .filter_map(|index| self.plan_node_output(left).identities.get(*index).cloned())
            .collect::<Vec<_>>();
        output_identities.extend(
            spec.right_projection
                .iter()
                .filter_map(|index| self.plan_node_output(right).identities.get(*index).cloned()),
        );
        output_identities.resize(output_types.len(), ColumnIdentity::Internal);
        Ok(self.push_node(
            PhysicalNodeKind::NestedLoopJoin(Box::new(spec)),
            RowType::with_identities(output_names, output_types, output_identities),
            vec![left, right],
            OperatorLabel::new(join.left.id, "NESTED_LOOP_JOIN"),
            None,
        ))
    }
}

mod reduction;
use reduction::*;

fn partition_hash_join_conditions(
    join: &ComparisonJoin<PreparedChild>,
) -> (Box<[JoinCondition]>, Box<[JoinCondition]>) {
    let (keys, residuals): (Vec<_>, Vec<_>) = join
        .conditions
        .iter()
        .cloned()
        .partition(|condition| is_hash_join_comparison(condition.comparison));
    debug_assert!(!keys.is_empty(), "hash join requires an equality key");
    (keys.into_boxed_slice(), residuals.into_boxed_slice())
}

/// Prove build-key uniqueness from the current relational output rather than
/// tracing a column back to a base-table declaration. The shared proof tracks
/// duplicate-preserving projections, windows, aggregates, and key-preserving
/// joins; ordinary equality supplies the required NULL rejection.
fn hash_join_build_keys_are_declared_unique<P: paro_planner::logical::plan::LogicalInput>(
    build: &P,
    key_conditions: &[JoinCondition],
) -> bool {
    if key_conditions.is_empty()
        || key_conditions
            .iter()
            .any(|condition| condition.comparison != JoinComparisonType::Equal)
    {
        return false;
    }
    let expressions = key_conditions
        .iter()
        .map(|condition| &condition.right)
        .collect::<Vec<_>>();
    crate::estimate::unique_keys::expressions_cover_key_in_layout(
        &build.output_layout(),
        &build.node_stats().unique_keys,
        &expressions,
        Some(paro_planner::logical::plan::UniqueKeyProvenance::CatalogEnforced),
    )
}

/// Produce a speculative execution hint from the current storage snapshot.
/// The cached plan does not treat these bounds as a correctness fact: the
/// concurrent builder invalidates itself on any runtime domain/count drift and
/// hash-join finish falls back to the canonical retained-row path.
fn plan_build_time_integer_join_index(
    build: &PreparedNode,
    key_conditions: &[JoinCondition],
) -> Option<BuildTimeIntegerJoinIndexSpec> {
    let [condition] = key_conditions else {
        return None;
    };
    if condition.comparison != JoinComparisonType::Equal {
        return None;
    }
    let (get, column_id) = resolve_base_get_column(build, &condition.right)?;
    let table = get.table.as_ref()?;
    let storage = table.get_storage()?;
    let column_stats = storage.column_statistics(column_id)?;
    let (minimum, maximum) =
        paro_storage::statistics::NumericStats::guaranteed_bounds(column_stats.statistics())?;
    let estimated_rows = usize::try_from(storage.tablet().statistics().ok()?.num_rows).ok()?;
    (estimated_rows > 0).then_some(BuildTimeIntegerJoinIndexSpec {
        minimum,
        maximum,
        estimated_rows,
    })
}

/// Resolve one physical build-side output back to a stable base-table column.
///
/// Join conditions are positional `Reference`s after column binding resolution,
/// while catalog constraints and storage statistics use physical column ids.
/// Keeping this translation here makes uniqueness an explicit property of a
/// transparent unary carrier rather than an accident of expression bindings.
fn resolve_base_get_column<'a, P: LogicalPlanRead>(
    build: &'a P,
    expression: &Expression,
) -> Option<(&'a paro_planner::logical::operator::Get, usize)> {
    match expression {
        Expression::Reference(reference) => resolve_base_get_output(build, reference.index),
        Expression::ColumnRef(column) if column.depth == 0 => resolve_bound_get_column(
            build,
            column.binding.table_index,
            column.binding.column_index,
        ),
        _ => None,
    }
}

fn resolve_base_get_output<P: LogicalPlanRead>(
    build: &P,
    output_index: usize,
) -> Option<(&paro_planner::logical::operator::Get, usize)> {
    match build.operator() {
        LogicalOperator::Get(get) => Some((get, get.stored_column(output_index)?)),
        LogicalOperator::Filter(filter) => {
            let child_index = filter
                .projection_map
                .to_indices(filter.child.types().len())
                .get(output_index)
                .copied()?;
            resolve_base_get_output(&*filter.child, child_index)
        }
        LogicalOperator::Projection(projection)
            if !matches!(projection.child.operator(), LogicalOperator::RowFetch(_)) =>
        {
            resolve_base_get_column(
                &*projection.child,
                projection.expressions.get(output_index)?,
            )
        }
        LogicalOperator::Order(order) => {
            let child_index = order
                .projection_map
                .to_indices(order.child.types().len())
                .get(output_index)
                .copied()?;
            resolve_base_get_output(&*order.child, child_index)
        }
        LogicalOperator::Limit(limit) => resolve_base_get_output(&*limit.child, output_index),
        LogicalOperator::TopN(topn) => {
            let child_index = topn
                .projection_map
                .to_indices(topn.child.types().len())
                .get(output_index)
                .copied()?;
            resolve_base_get_output(&*topn.child, child_index)
        }
        LogicalOperator::Join(Join::Comparison(join))
            if join.join_type == JoinType::Inner
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped =>
        {
            let left_projection = join.left_projection_map.to_indices(join.left.types().len());
            if let Some(&child_index) = left_projection.get(output_index) {
                return resolve_base_get_output(&*join.left, child_index);
            }
            let right_output = output_index.checked_sub(left_projection.len())?;
            let right_projection = join
                .right_projection_map
                .to_indices(join.right.types().len());
            resolve_base_get_output(&*join.right, *right_projection.get(right_output)?)
        }
        _ => None,
    }
}

fn resolve_bound_get_column<P: LogicalPlanRead>(
    build: &P,
    table_index: usize,
    column_index: usize,
) -> Option<(&paro_planner::logical::operator::Get, usize)> {
    match build.operator() {
        LogicalOperator::Get(get) if get.table_index == table_index => {
            Some((get, get.stored_column(column_index)?))
        }
        LogicalOperator::Filter(filter) => {
            resolve_bound_get_column(&*filter.child, table_index, column_index)
        }
        LogicalOperator::Projection(projection)
            if !matches!(projection.child.operator(), LogicalOperator::RowFetch(_)) =>
        {
            resolve_bound_get_column(&*projection.child, table_index, column_index)
        }
        LogicalOperator::Order(order) => {
            resolve_bound_get_column(&*order.child, table_index, column_index)
        }
        LogicalOperator::Limit(limit) => {
            resolve_bound_get_column(&*limit.child, table_index, column_index)
        }
        LogicalOperator::TopN(topn) => {
            resolve_bound_get_column(&*topn.child, table_index, column_index)
        }
        LogicalOperator::Join(Join::Comparison(join))
            if join.join_type == JoinType::Inner
                && join.duplicate_eliminated_columns.is_empty()
                && !join.delim_flipped =>
        {
            let left = resolve_bound_get_column(&*join.left, table_index, column_index);
            let right = resolve_bound_get_column(&*join.right, table_index, column_index);
            match (left, right) {
                (Some(column), None) | (None, Some(column)) => Some(column),
                // A binding namespace must identify exactly one source in a
                // transparent carrier. Decline rather than guessing if an
                // invalid plan aliases a table index across both children.
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
