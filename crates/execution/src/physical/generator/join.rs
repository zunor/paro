// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_join(
        &mut self,
        join: &Join,
        join_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        match join {
            Join::Comparison(comparison) => {
                self.lower_comparison_join(comparison, join_cardinality)
            }
            Join::Any(any) => self.lower_any_join(any),
            Join::Cross(cross) => self.lower_cross_product(cross),
        }
    }

    pub(crate) fn lower_comparison_join(
        &mut self,
        join: &ComparisonJoin,
        join_cardinality: Option<paro_planner::plan::CardinalityEstimate>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if let Some(cascade) = self.try_lower_reduction_cascade(join)? {
            return Ok(cascade);
        }
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
            return self.unsupported_preserving_children(
                "JOIN",
                format!("typed join does not support {} join", join.join_type),
                &[join.left.as_ref(), join.right.as_ref()],
            );
        }
        if !join.duplicate_eliminated_columns.is_empty() || join.delim_flipped {
            return self.lower_comparison_delim_join(join);
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
        join: &ComparisonJoin,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.generate_node(join.left.as_ref())?;
        let right = self.generate_node(join.right.as_ref())?;
        let left_projection = nlj_left_projection(join);
        let right_projection = nlj_right_projection(join);
        let left_names = project_by_index(
            &join.left.output_names(),
            &left_projection,
            "nlj left output",
        )?;
        let left_types = project_by_index(&join.left.types(), &left_projection, "nlj left")?;
        let right_names = project_by_index(
            &join.right.output_names(),
            &right_projection,
            "nlj right output",
        )?;
        let right_types = project_by_index(&join.right.types(), &right_projection, "nlj right")?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let spec = NestedLoopJoinSpec {
            join_type: join.join_type,
            conditions: join.conditions.clone().into_boxed_slice(),
            mark_null_condition_start: join.mark_null_condition_start,
            arbitrary_condition: None,
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::NestedLoopJoin(spec), vec![left, right]))
    }

    pub(crate) fn lower_sort_range_join(
        &mut self,
        join: &ComparisonJoin,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.generate_node(join.left.as_ref())?;
        let right = self.generate_node(join.right.as_ref())?;
        let left_projection = nlj_left_projection(join);
        let right_projection = nlj_right_projection(join);
        let left_names = project_by_index(
            &join.left.output_names(),
            &left_projection,
            "sort-range join left output",
        )?;
        let left_types =
            project_by_index(&join.left.types(), &left_projection, "sort-range join left")?;
        let right_names = project_by_index(
            &join.right.output_names(),
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
            mark_null_condition_start: join.mark_null_condition_start,
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
        join: &ComparisonJoin,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.generate_node(join.left.as_ref())?;
        let right = self.generate_node(join.right.as_ref())?;
        let left_projection = nlj_left_projection(join);
        let right_projection = nlj_right_projection(join);
        let left_names = project_by_index(
            &join.left.output_names(),
            &left_projection,
            "classic IE join left output",
        )?;
        let left_types =
            project_by_index(&join.left.types(), &left_projection, "classic IE join left")?;
        let right_names = project_by_index(
            &join.right.output_names(),
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
            mark_null_condition_start: join.mark_null_condition_start,
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
        any: &paro_planner::operator::join::AnyJoin,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if !supports_typed_hash_join_type(any.join_type) {
            return self.unsupported_preserving_children(
                "JOIN",
                format!("typed join does not support {} join", any.join_type),
                &[any.left.as_ref(), any.right.as_ref()],
            );
        }
        let left = self.generate_node(any.left.as_ref())?;
        let right = self.generate_node(any.right.as_ref())?;
        let left_projection = any.left_projection_map.to_indices(any.left.types().len());
        let right_projection = any.right_projection_map.to_indices(any.right.types().len());
        let left_names =
            project_by_index(&any.left.output_names(), &left_projection, "any join left")?;
        let left_types = project_by_index(&any.left.types(), &left_projection, "any join left")?;
        let right_names = project_by_index(
            &any.right.output_names(),
            &right_projection,
            "any join right",
        )?;
        let right_types =
            project_by_index(&any.right.types(), &right_projection, "any join right")?;
        let output_names = join_output_names(any.join_type, left_names, right_names);
        let output_types = any.get_types();
        let spec = NestedLoopJoinSpec {
            join_type: any.join_type,
            conditions: Box::new([]),
            mark_null_condition_start: any.join_type.eq(&JoinType::Mark).then_some(0),
            arbitrary_condition: Some(any.condition.clone()),
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::NestedLoopJoin(spec), vec![left, right]))
    }

    pub(crate) fn lower_cross_product(
        &mut self,
        cross: &CrossProduct,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.generate_node(cross.left.as_ref())?;
        let right = self.generate_node(cross.right.as_ref())?;
        let mut output_names = cross.left.output_names();
        output_names.extend(cross.right.output_names());
        let output_types = cross.get_types();
        let spec = CrossProductSpec {
            left_output_types: cross.left.types().into_boxed_slice(),
            right_output_types: cross.right.types().into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::CrossProduct(spec), vec![left, right]))
    }

    pub(crate) fn lower_comparison_hash_join(
        &mut self,
        join: &ComparisonJoin,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let left = self.generate_node(join.left.as_ref())?;
        let right = self.generate_node(join.right.as_ref())?;
        let left_projection = hash_join_left_projection(join);
        let right_projection = hash_join_right_projection(join);
        let left_names = project_by_index(
            &join.left.output_names(),
            &left_projection,
            "hash join left output",
        )?;
        let left_types = project_by_index(&join.left.types(), &left_projection, "hash join left")?;
        let right_names = project_by_index(
            &join.right.output_names(),
            &right_projection,
            "hash join right output",
        )?;
        let right_types =
            project_by_index(&join.right.types(), &right_projection, "hash join right")?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let (key_conditions, residual_conditions) = partition_hash_join_conditions(join);
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
            key_conditions,
            residual_conditions,
            left_projection: left_projection.into_boxed_slice(),
            build_input_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            build_output_count,
            build_payload_types: build_payload_types.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
            force_external: self.ctx.force_external
                && supports_external_hash_join_type(join.join_type),
            reduction_cascade: None,
        };
        Ok((PhysicalNodeKind::HashJoin(spec), vec![left, right]))
    }

    /// Fuse consecutive build-preserving existential reductions that read the
    /// same base relation. The filtering aliases are semantically independent,
    /// but their scans need not be: one candidate stream can classify every
    /// reduction while the preserved relation is stored once.
    fn try_lower_reduction_cascade(
        &mut self,
        root: &ComparisonJoin,
    ) -> Result<Option<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)>> {
        let mut joins = Vec::new();
        let mut current = root;
        loop {
            if !matches!(current.join_type, JoinType::RightSemi | JoinType::RightAnti)
                || current.anti_join_mode != AntiJoinMode::Regular
                || !current.left_projection_map.is_none()
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
            for column_id in branch.get.column_ids.iter().copied() {
                if !merged_column_ids.contains(&column_id) {
                    merged_column_ids.push(column_id);
                }
            }
        }
        let mut merged_get = branches[0].get.clone();
        merged_get.column_ids = merged_column_ids.clone();
        merged_get.names = merged_column_ids
            .iter()
            .map(|&column_id| first_table.columns[column_id].name.clone())
            .collect();
        merged_get.column_types = merged_column_ids
            .iter()
            .map(|&column_id| first_table.columns[column_id].logical_type.clone())
            .collect();
        merged_get.returned_types = merged_get.column_types.clone();
        let Some(runtime_filter_expressions) = branches
            .iter()
            .flat_map(|branch| {
                branch
                    .get
                    .runtime_filter_expressions
                    .iter()
                    .map(move |expression| (branch, expression))
            })
            .map(|(branch, expression)| {
                remap_reduction_expression(
                    expression,
                    branch_column_ids(branch.get),
                    branch.get.table_index,
                    &merged_column_ids,
                    merged_get.table_index,
                )
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        merged_get.runtime_filter_expressions = runtime_filter_expressions;

        let mut common_keys: Option<Vec<JoinCondition>> = None;
        let mut reduction_steps = Vec::with_capacity(joins.len());
        let mut reduction_predicates: Vec<HashReductionPredicateSpec> = Vec::new();
        let mut reduction_source_predicates: Vec<HashReductionSourcePredicateSpec> = Vec::new();
        let mut build_residual_types = Vec::new();
        let mut required_mask = 0u8;
        let mut forbidden_mask = 0u8;

        for (step_idx, (join, branch)) in joins.iter().zip(&branches).enumerate() {
            let mut keys = Vec::new();
            let mut residuals = Vec::new();
            for condition in &join.conditions {
                let Some(left) = remap_reduction_expression(
                    &condition.left,
                    &branch.output_column_ids,
                    branch.get.table_index,
                    &merged_column_ids,
                    merged_get.table_index,
                ) else {
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
                let predicate_idx = if let Some(predicate_idx) = reduction_predicates
                    .iter()
                    .position(|existing| same_reduction_condition(&existing.condition, &residual))
                {
                    predicate_idx
                } else {
                    let predicate_idx = reduction_predicates.len();
                    if predicate_idx >= u8::BITS as usize {
                        return Ok(None);
                    }
                    let predicate_bit = 1u8 << predicate_idx;
                    build_residual_types.push(residual.right.return_type());
                    reduction_predicates.push(HashReductionPredicateSpec {
                        condition: residual,
                        build_residual_offset: predicate_idx,
                        predicate_mask: predicate_bit,
                    });
                    predicate_idx
                };
                if predicate_idx >= u8::BITS as usize {
                    return Ok(None);
                }
                predicate_mask |= 1u8 << predicate_idx;
            }
            for filter in &branch.filters {
                let Some(expression) = remap_reduction_expression(
                    filter,
                    branch_column_ids(branch.get),
                    branch.get.table_index,
                    &merged_column_ids,
                    merged_get.table_index,
                ) else {
                    return Ok(None);
                };
                let predicate_idx = reduction_predicates.len() + reduction_source_predicates.len();
                if predicate_idx >= u8::BITS as usize {
                    return Ok(None);
                }
                let source_mask = 1u8 << predicate_idx;
                reduction_source_predicates.push(HashReductionSourcePredicateSpec {
                    expression,
                    predicate_mask: source_mask,
                });
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
        let build = self.generate_node(preserved)?;
        let preserved_types = preserved.types();
        let build_input_projection = root.right_projection_map.to_indices(preserved_types.len());
        let mut build_payload_types = project_by_index(
            &preserved_types,
            &build_input_projection,
            "reduction cascade preserved output",
        )?;
        let build_output_count = build_payload_types.len();
        build_payload_types.extend(build_residual_types);
        let spec = HashJoinSpec {
            join_type: JoinType::RightSemi,
            anti_join_mode: AntiJoinMode::Regular,
            key_conditions: key_conditions.into_boxed_slice(),
            residual_conditions: Box::new([]),
            left_projection: Box::new([]),
            build_input_projection: build_input_projection.into_boxed_slice(),
            left_output_types: Box::new([]),
            build_output_count,
            build_payload_types: build_payload_types.into_boxed_slice(),
            output_names: comparison_join_output_names(root)?.into_boxed_slice(),
            output_types: root.get_types().into_boxed_slice(),
            force_external: self.ctx.force_external,
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
        join: &ComparisonJoin,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if join.duplicate_eliminated_columns.is_empty() {
            return self.unsupported_preserving_children(
                "JOIN",
                "delim-flipped comparison join is missing duplicate-eliminated keys",
                &[join.left.as_ref(), join.right.as_ref()],
            );
        }
        let (side, capture_input, wrapped_left, wrapped_right, cached_outer_output) =
            if join.delim_flipped {
                (
                    DelimJoinSideSpec::Right,
                    self.generate_node(join.right.as_ref())?,
                    self.generate_node(join.left.as_ref())?,
                    self.synthetic_cached_outer_scan(
                        join.right.output_names(),
                        join.right.types(),
                        join.right.id,
                    ),
                    RowType::new(join.right.output_names(), join.right.types()),
                )
            } else {
                (
                    DelimJoinSideSpec::Left,
                    self.generate_node(join.left.as_ref())?,
                    self.synthetic_cached_outer_scan(
                        join.left.output_names(),
                        join.left.types(),
                        join.left.id,
                    ),
                    self.generate_node(join.right.as_ref())?,
                    RowType::new(join.left.output_names(), join.left.types()),
                )
            };

        let has_hash_key = join
            .conditions
            .iter()
            .any(|condition| is_hash_join_comparison(condition.comparison));
        let mark_has_residual = join.join_type == JoinType::Mark
            && join
                .conditions
                .iter()
                .any(|condition| !is_hash_join_comparison(condition.comparison));
        let mark_needs_scoped_nulls =
            matches!(join.mark_null_condition_start, Some(start) if start > 0);
        let wrapped_join = if has_hash_key && !mark_has_residual && !mark_needs_scoped_nulls {
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
        output_names: Vec<String>,
        output_types: Vec<LogicalType>,
        logical_id: paro_planner::plan::PlanNodeId,
    ) -> PhysicalPlanNodeId {
        let spec = DelimScanSpec {
            target: DelimScanTarget::CachedOuter,
            output_names: output_names.clone().into_boxed_slice(),
            output_types: output_types.clone().into_boxed_slice(),
        };
        self.push_node(
            PhysicalNodeKind::DelimScan(spec),
            RowType::new(output_names, output_types),
            Vec::new(),
            OperatorLabel::new(logical_id, "DELIM_CACHED_OUTER"),
            None,
        )
    }

    pub(crate) fn push_wrapped_hash_join(
        &mut self,
        join: &ComparisonJoin,
        left: PhysicalPlanNodeId,
        right: PhysicalPlanNodeId,
    ) -> Result<PhysicalPlanNodeId> {
        let left_projection = hash_join_left_projection(join);
        let right_projection = hash_join_right_projection(join);
        let left_names = project_by_index(
            &join.left.output_names(),
            &left_projection,
            "delim hash join left output",
        )?;
        let left_types =
            project_by_index(&join.left.types(), &left_projection, "delim hash join left")?;
        let right_names = project_by_index(
            &join.right.output_names(),
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
        let (key_conditions, residual_conditions) = partition_hash_join_conditions(join);
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
            key_conditions,
            residual_conditions,
            left_projection: left_projection.into_boxed_slice(),
            build_input_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            build_output_count,
            build_payload_types: build_payload_types.into_boxed_slice(),
            output_names: output_names.clone().into_boxed_slice(),
            output_types: output_types.clone().into_boxed_slice(),
            force_external: self.ctx.force_external
                && supports_external_hash_join_type(join.join_type),
            reduction_cascade: None,
        };
        Ok(self.push_node(
            PhysicalNodeKind::HashJoin(spec),
            RowType::new(output_names, output_types),
            vec![left, right],
            OperatorLabel::new(join.left.id, "HASH_JOIN"),
            None,
        ))
    }

    pub(crate) fn push_wrapped_nlj(
        &mut self,
        join: &ComparisonJoin,
        left: PhysicalPlanNodeId,
        right: PhysicalPlanNodeId,
    ) -> Result<PhysicalPlanNodeId> {
        let left_projection = nlj_left_projection(join);
        let right_projection = nlj_right_projection(join);
        let left_names = project_by_index(
            &join.left.output_names(),
            &left_projection,
            "delim nlj left output",
        )?;
        let left_types = project_by_index(&join.left.types(), &left_projection, "delim nlj left")?;
        let right_names = project_by_index(
            &join.right.output_names(),
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
            mark_null_condition_start: join.mark_null_condition_start,
            arbitrary_condition: None,
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.clone().into_boxed_slice(),
            output_types: output_types.clone().into_boxed_slice(),
        };
        Ok(self.push_node(
            PhysicalNodeKind::NestedLoopJoin(spec),
            RowType::new(output_names, output_types),
            vec![left, right],
            OperatorLabel::new(join.left.id, "NESTED_LOOP_JOIN"),
            None,
        ))
    }
}

fn plan_grouped_extrema_reduction(
    key_conditions: &[JoinCondition],
    predicates: &[HashReductionPredicateSpec],
    source_predicates: &[HashReductionSourcePredicateSpec],
    steps: &[HashReductionStepSpec],
) -> Option<HashReductionGroupedExtremaSpec> {
    let [key] = key_conditions else {
        return None;
    };
    let [predicate] = predicates else {
        return None;
    };
    if key.comparison != JoinComparisonType::Equal
        || key.left.return_type() != LogicalType::BigInt
        || key.right.return_type() != LogicalType::BigInt
        || predicate.condition.comparison != JoinComparisonType::NotEqual
        || predicate.condition.left.return_type() != LogicalType::BigInt
        || predicate.condition.right.return_type() != LogicalType::BigInt
    {
        return None;
    }
    let source_value_index = match &predicate.condition.left {
        Expression::Reference(reference) => reference.index,
        Expression::ColumnRef(column) => column.binding.column_index,
        _ => return None,
    };

    let source_bits = source_predicates
        .iter()
        .fold(0u8, |mask, predicate| mask | predicate.predicate_mask);
    let mut channels: Vec<HashReductionExtremaChannelSpec> = Vec::new();
    for step in steps {
        if step.predicate_mask & predicate.predicate_mask != predicate.predicate_mask {
            return None;
        }
        let source_predicate_mask = step.predicate_mask & !predicate.predicate_mask;
        if source_predicate_mask & !source_bits != 0 {
            return None;
        }
        if let Some(channel) = channels
            .iter_mut()
            .find(|channel| channel.source_predicate_mask == source_predicate_mask)
        {
            channel.match_mask |= step.match_mask;
        } else {
            channels.push(HashReductionExtremaChannelSpec {
                source_predicate_mask,
                match_mask: step.match_mask,
            });
        }
    }
    Some(HashReductionGroupedExtremaSpec {
        source_value_index,
        build_residual_offset: predicate.build_residual_offset,
        channels: channels.into_boxed_slice(),
    })
}

struct ReductionScanBranch<'a> {
    get: &'a Get,
    /// Base-table column id for each column exposed to the reduction join.
    output_column_ids: Vec<usize>,
    /// Predicates local to this logical alias, still bound to the Get input.
    filters: Vec<Expression>,
}

impl<'a> ReductionScanBranch<'a> {
    fn inspect(plan: &'a LogicalPlan) -> Option<Self> {
        match &plan.operator {
            LogicalOperator::Get(get) => Some(Self {
                get,
                output_column_ids: get.column_ids.clone(),
                filters: Vec::new(),
            }),
            LogicalOperator::Filter(filter) => {
                let LogicalOperator::Get(get) = &filter.child.operator else {
                    return None;
                };
                let output_column_ids = filter
                    .projection_map
                    .to_indices(get.returned_types.len())
                    .into_iter()
                    .map(|index| get.column_ids.get(index).copied())
                    .collect::<Option<Vec<_>>>()?;
                Some(Self {
                    get,
                    output_column_ids,
                    filters: filter.expressions.clone(),
                })
            }
            _ => None,
        }
    }
}

fn branch_column_ids(get: &Get) -> &[usize] {
    &get.column_ids
}

fn remap_reduction_expression(
    expression: &Expression,
    source_column_ids: &[usize],
    source_table_index: usize,
    target_column_ids: &[usize],
    target_table_index: usize,
) -> Option<Expression> {
    fn remap(
        expression: &mut Expression,
        source_column_ids: &[usize],
        source_table_index: usize,
        target_column_ids: &[usize],
        target_table_index: usize,
    ) -> bool {
        match expression {
            Expression::ColumnRef(column) => {
                if column.binding.table_index != source_table_index {
                    return false;
                }
                let Some(column_id) = source_column_ids.get(column.binding.column_index) else {
                    return false;
                };
                let Some(target_index) = target_column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                else {
                    return false;
                };
                *column = ColumnRefExpression::new(
                    paro_planner::operator::ColumnBinding::new(target_table_index, target_index),
                    column.return_type.clone(),
                );
                true
            }
            Expression::Reference(reference) => {
                let Some(column_id) = source_column_ids.get(reference.index) else {
                    return false;
                };
                let Some(target_index) = target_column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                else {
                    return false;
                };
                reference.index = target_index;
                true
            }
            _ => {
                let mut valid = true;
                ExpressionIterator::enumerate_children_mut(expression, |child| {
                    valid &= remap(
                        child,
                        source_column_ids,
                        source_table_index,
                        target_column_ids,
                        target_table_index,
                    );
                });
                valid
            }
        }
    }

    let mut expression = expression.clone();
    remap(
        &mut expression,
        source_column_ids,
        source_table_index,
        target_column_ids,
        target_table_index,
    )
    .then_some(expression)
}

fn same_reduction_keys(left: &[JoinCondition], right: &[JoinCondition]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.comparison == right.comparison
                && same_reduction_key_expression(&left.left, &right.left)
                && same_reduction_key_expression(&left.right, &right.right)
        })
}

fn same_reduction_condition(left: &JoinCondition, right: &JoinCondition) -> bool {
    left.comparison == right.comparison
        && same_reduction_predicate_expression(&left.left, &right.left)
        && same_reduction_predicate_expression(&left.right, &right.right)
}

fn same_reduction_predicate_expression(left: &Expression, right: &Expression) -> bool {
    if same_reduction_key_expression(left, right) {
        return true;
    }
    match (left, right) {
        (Expression::Constant(left), Expression::Constant(right)) => {
            left.return_type == right.return_type && left.value == right.value
        }
        _ => false,
    }
}

fn same_reduction_key_expression(left: &Expression, right: &Expression) -> bool {
    match (left, right) {
        (Expression::Reference(left), Expression::Reference(right)) => {
            left.index == right.index && left.return_type == right.return_type
        }
        (Expression::ColumnRef(left), Expression::ColumnRef(right)) => {
            left.binding.column_index == right.binding.column_index
                && left.return_type == right.return_type
        }
        _ => false,
    }
}

fn partition_hash_join_conditions(
    join: &ComparisonJoin,
) -> (Box<[JoinCondition]>, Box<[JoinCondition]>) {
    let (keys, residuals): (Vec<_>, Vec<_>) = join
        .conditions
        .iter()
        .cloned()
        .partition(|condition| is_hash_join_comparison(condition.comparison));
    debug_assert!(!keys.is_empty(), "hash join requires an equality key");
    (keys.into_boxed_slice(), residuals.into_boxed_slice())
}
