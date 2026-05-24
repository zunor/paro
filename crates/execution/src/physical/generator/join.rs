// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_join(
        &mut self,
        join: &Join,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        match join {
            Join::Comparison(comparison) => self.lower_comparison_join(comparison),
            Join::Any(any) => self.lower_any_join(any),
            Join::Cross(cross) => self.lower_cross_product(cross),
        }
    }

    pub(crate) fn lower_comparison_join(
        &mut self,
        join: &ComparisonJoin,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
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
        let all_hashable = !join.conditions.is_empty()
            && join
                .conditions
                .iter()
                .all(|c| is_hash_join_comparison(c.comparison));
        if all_hashable {
            return self.lower_comparison_hash_join(join);
        }
        if is_ie_join_candidate(&join.conditions) {
            return self.lower_ie_join(join);
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

    pub(crate) fn lower_ie_join(
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
            "iejoin left output",
        )?;
        let left_types = project_by_index(&join.left.types(), &left_projection, "iejoin left")?;
        let right_names = project_by_index(
            &join.right.output_names(),
            &right_projection,
            "iejoin right output",
        )?;
        let right_types = project_by_index(&join.right.types(), &right_projection, "iejoin right")?;
        let output_names = join_output_names(join.join_type, left_names, right_names);
        let output_types = join.get_types();
        let spec = IEJoinSpec {
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
        Ok((PhysicalNodeKind::IEJoin(spec), vec![left, right]))
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
        let left_projection = if any.left_projection_map.is_empty() {
            (0..any.left.types().len()).collect()
        } else {
            any.left_projection_map.clone()
        };
        let right_projection = if any.right_projection_map.is_empty() {
            (0..any.right.types().len()).collect()
        } else {
            any.right_projection_map.clone()
        };
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
        let spec = HashJoinSpec {
            join_type: join.join_type,
            conditions: join.conditions.clone().into_boxed_slice(),
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.into_boxed_slice(),
            output_types: output_types.into_boxed_slice(),
            force_external: self.ctx.force_external
                && supports_external_hash_join_type(join.join_type),
        };
        Ok((PhysicalNodeKind::HashJoin(spec), vec![left, right]))
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

        let all_hashable = !join.conditions.is_empty()
            && join
                .conditions
                .iter()
                .all(|c| is_hash_join_comparison(c.comparison));
        let mark_needs_scoped_nulls =
            matches!(join.mark_null_condition_start, Some(start) if start > 0);
        let wrapped_join = if all_hashable && !mark_needs_scoped_nulls {
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
        let spec = HashJoinSpec {
            join_type: join.join_type,
            conditions: join.conditions.clone().into_boxed_slice(),
            left_projection: left_projection.into_boxed_slice(),
            right_projection: right_projection.into_boxed_slice(),
            left_output_types: left_types.into_boxed_slice(),
            right_output_types: right_types.into_boxed_slice(),
            output_names: output_names.clone().into_boxed_slice(),
            output_types: output_types.clone().into_boxed_slice(),
            force_external: self.ctx.force_external
                && supports_external_hash_join_type(join.join_type),
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
