// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::pipeline::graph::{MaterializeSinkSpec, NljUnmatchedSourceSpec};

impl<'a> PipelineLowerer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_cross_product_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &CrossProductSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let node = self.plan.node(root);
        let children = self.plan.child_ids(&node.children);
        let [left, right] = children else {
            return Err(paro_error::internal(format!(
                "{} expected exactly two cross product children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let (producer, handle) =
            self.lower_cross_product_build(*right, spec, pipelines, dependencies)?;

        let (source, mut transforms, pending_builds) =
            self.collect_probe_roles(*left, pipelines, dependencies)?;
        let source_handles = source.clone();
        transforms.push(cross_product_probe_transform(handle, spec));
        transforms.extend(consumer_transforms);

        let pushed = self.push_pipeline(
            source,
            transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        let consumer = pushed.entry;
        self.add_source_handle_dependencies(&source_handles, consumer, dependencies)?;
        for pending in &pending_builds {
            self.handles.add_consumer(pending.handle, consumer)?;
        }
        self.handles.add_consumer(handle, consumer)?;
        dependencies.extend(
            pending_builds
                .into_iter()
                .map(|pending| PipelineDependency {
                    producer: pending.producer,
                    consumer,
                    kind: DependencyKind::BuildBeforeProbe,
                }),
        );
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::BuildBeforeProbe,
        });
        Ok(pushed.tail)
    }

    pub(crate) fn lower_cross_product_build(
        &mut self,
        right: PhysicalPlanNodeId,
        spec: &CrossProductSpec,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<(PipelineId, BreakerHandleId)> {
        let right_output = RowType::new(
            (0..spec.right_output_types.len())
                .map(|idx| format!("cross_build_{}", idx + 1))
                .collect(),
            spec.right_output_types.to_vec(),
        );
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            right_output,
            Default::default(),
        );
        let producer = self.lower_subtree_to_sink(
            right,
            SinkSpec::CrossProductBuild(CrossProductBuildSinkSpec {
                handle,
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            self.plan.node(right).output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;
        Ok((producer, handle))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_nested_loop_join_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &NestedLoopJoinSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let needs_unmatched = needs_nlj_unmatched_source(spec.join_type);
        let downstream_is_client = matches!(sink, SinkSpec::ClientResult(_))
            && matches!(sink_sharing, SinkSharing::Exclusive);
        if needs_unmatched
            && consumer_transforms
                .iter()
                .any(|t| matches!(t, TransformSpec::Limit(_)))
        {
            return Err(paro_error::not_implemented(
                "LIMIT above right/full nested loop join requires a shared post-join limit pipeline",
            ));
        }
        let branch_sharing = if downstream_is_client {
            SinkSharing::Exclusive
        } else {
            match sink_sharing {
                SinkSharing::Exclusive => {
                    if needs_unmatched {
                        SinkSharing::Shared(self.next_shared_sink())
                    } else {
                        SinkSharing::Exclusive
                    }
                }
                shared @ SinkSharing::Shared(_) => shared,
            }
        };

        let node = self.plan.node(root);
        let children = self.plan.child_ids(&node.children);
        let [left, right] = children else {
            return Err(paro_error::internal(format!(
                "{} expected exactly two NLJ children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let (producer, handle) = self.lower_nlj_build(*right, spec, pipelines, dependencies)?;

        let (source, mut transforms, pending_builds) =
            self.collect_probe_roles(*left, pipelines, dependencies)?;
        let source_handles = source.clone();
        transforms.push(nlj_probe_transform(handle, spec));
        transforms.extend(consumer_transforms.iter().cloned());

        let pushed = self.push_pipeline(
            source,
            transforms,
            sink.clone(),
            branch_sharing,
            output.clone(),
            pipelines,
            dependencies,
        )?;
        let consumer = pushed.entry;
        self.add_source_handle_dependencies(&source_handles, consumer, dependencies)?;
        for pending in &pending_builds {
            self.handles.add_consumer(pending.handle, consumer)?;
        }
        self.handles.add_consumer(handle, consumer)?;
        dependencies.extend(
            pending_builds
                .into_iter()
                .map(|pending| PipelineDependency {
                    producer: pending.producer,
                    consumer,
                    kind: DependencyKind::BuildBeforeProbe,
                }),
        );
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::BuildBeforeProbe,
        });

        if needs_unmatched {
            let unmatched_source = SourceSpec::NljUnmatched(NljUnmatchedSourceSpec {
                handle,
                join_type: spec.join_type,
                left_output_types: spec.left_output_types.clone(),
                right_projection: spec.right_projection.clone(),
                output_names: spec.output_names.clone(),
                output_types: spec.output_types.clone(),
            });
            let unmatched = self.push_pipeline(
                unmatched_source,
                consumer_transforms,
                sink,
                branch_sharing,
                output,
                pipelines,
                dependencies,
            )?;
            self.handles.add_consumer(handle, unmatched.entry)?;
            dependencies.push(PipelineDependency {
                producer: consumer,
                consumer: unmatched.entry,
                kind: DependencyKind::FinalizeBeforeEmit,
            });
            return Ok(unmatched.tail);
        }

        Ok(pushed.tail)
    }

    pub(crate) fn lower_nlj_build(
        &mut self,
        right: PhysicalPlanNodeId,
        spec: &NestedLoopJoinSpec,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<(PipelineId, BreakerHandleId)> {
        let right_output = RowType::new(
            (0..spec.right_output_types.len())
                .map(|idx| format!("nlj_build_{}", idx + 1))
                .collect(),
            spec.right_output_types.to_vec(),
        );
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            right_output,
            Default::default(),
        );
        let producer = self.lower_subtree_to_sink(
            right,
            SinkSpec::Materialize(MaterializeSinkSpec {
                handle,
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            self.plan.node(right).output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;
        Ok((producer, handle))
    }

    pub(crate) fn lower_sort_range_build(
        &mut self,
        right: PhysicalPlanNodeId,
        spec: &SortRangeJoinSpec,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<(PipelineId, BreakerHandleId)> {
        let right_output = RowType::new(
            (0..spec.right_output_types.len())
                .map(|idx| format!("sort_range_build_{}", idx + 1))
                .collect(),
            spec.right_output_types.to_vec(),
        );
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            right_output,
            Default::default(),
        );
        let producer = self.lower_subtree_to_sink(
            right,
            SinkSpec::Materialize(MaterializeSinkSpec {
                handle,
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            self.plan.node(right).output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;
        Ok((producer, handle))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_sort_range_join_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &SortRangeJoinSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let needs_unmatched = needs_nlj_unmatched_source(spec.join_type);
        let downstream_is_client = matches!(sink, SinkSpec::ClientResult(_))
            && matches!(sink_sharing, SinkSharing::Exclusive);
        if needs_unmatched
            && consumer_transforms
                .iter()
                .any(|t| matches!(t, TransformSpec::Limit(_)))
        {
            return Err(paro_error::not_implemented(
                "LIMIT above right/full sort-range join requires a shared post-join limit pipeline",
            ));
        }
        let branch_sharing = if downstream_is_client {
            SinkSharing::Exclusive
        } else {
            match sink_sharing {
                SinkSharing::Exclusive => {
                    if needs_unmatched {
                        SinkSharing::Shared(self.next_shared_sink())
                    } else {
                        SinkSharing::Exclusive
                    }
                }
                shared @ SinkSharing::Shared(_) => shared,
            }
        };

        let node = self.plan.node(root);
        let children = self.plan.child_ids(&node.children);
        let [left, right] = children else {
            return Err(paro_error::internal(format!(
                "{} expected exactly two sort-range join children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let (producer, handle) =
            self.lower_sort_range_build(*right, spec, pipelines, dependencies)?;

        let (source, mut transforms, pending_builds) =
            self.collect_probe_roles(*left, pipelines, dependencies)?;
        let source_handles = source.clone();
        transforms.push(sort_range_probe_transform(handle, spec));
        transforms.extend(consumer_transforms.iter().cloned());

        let pushed = self.push_pipeline(
            source,
            transforms,
            sink.clone(),
            branch_sharing,
            output.clone(),
            pipelines,
            dependencies,
        )?;
        let consumer = pushed.entry;
        self.add_source_handle_dependencies(&source_handles, consumer, dependencies)?;
        for pending in &pending_builds {
            self.handles.add_consumer(pending.handle, consumer)?;
        }
        self.handles.add_consumer(handle, consumer)?;
        dependencies.extend(
            pending_builds
                .into_iter()
                .map(|pending| PipelineDependency {
                    producer: pending.producer,
                    consumer,
                    kind: DependencyKind::BuildBeforeProbe,
                }),
        );
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::BuildBeforeProbe,
        });

        if needs_unmatched {
            let unmatched_source = SourceSpec::NljUnmatched(NljUnmatchedSourceSpec {
                handle,
                join_type: spec.join_type,
                left_output_types: spec.left_output_types.clone(),
                right_projection: spec.right_projection.clone(),
                output_names: spec.output_names.clone(),
                output_types: spec.output_types.clone(),
            });
            let unmatched = self.push_pipeline(
                unmatched_source,
                consumer_transforms,
                sink,
                branch_sharing,
                output,
                pipelines,
                dependencies,
            )?;
            self.handles.add_consumer(handle, unmatched.entry)?;
            dependencies.push(PipelineDependency {
                producer: consumer,
                consumer: unmatched.entry,
                kind: DependencyKind::FinalizeBeforeEmit,
            });
            return Ok(unmatched.tail);
        }

        Ok(pushed.tail)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lower_hash_join_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        spec: &HashJoinSpec,
        consumer_transforms: Vec<TransformSpec>,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        let needs_unmatched = needs_hash_join_unmatched_source(spec.join_type);
        let downstream_is_client = matches!(sink, SinkSpec::ClientResult(_))
            && matches!(sink_sharing, SinkSharing::Exclusive);
        let branch_sharing = if downstream_is_client {
            SinkSharing::Exclusive
        } else {
            match sink_sharing {
                SinkSharing::Exclusive => SinkSharing::Shared(self.next_shared_sink()),
                shared @ SinkSharing::Shared(_) => shared,
            }
        };
        if needs_unmatched
            && consumer_transforms
                .iter()
                .any(|transform| matches!(transform, TransformSpec::Limit(_)))
        {
            return Err(paro_error::not_implemented(
                "LIMIT above right/full hash join requires a shared post-join limit pipeline",
            ));
        }

        let node = self.plan.node(root);
        let children = self.plan.child_ids(&node.children);
        let [left, right] = children else {
            return Err(paro_error::internal(format!(
                "{} expected exactly two join children, got {}",
                node.label.display_name,
                children.len()
            )));
        };

        let join_output = RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec());
        let handle = self.handles.register(
            BreakerHandleKind::HashJoinBuild,
            join_output,
            Default::default(),
        );

        let producer = self.lower_subtree_to_sink(
            *right,
            SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                handle,
                join_type: spec.join_type,
                conditions: spec.conditions.clone(),
                build_projection: spec.right_projection.clone(),
                build_payload_types: spec.right_output_types.clone(),
                required: Default::default(),
                force_external: spec.force_external,
            }),
            SinkSharing::Exclusive,
            self.plan.node(*right).output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;

        let (source, mut transforms, pending_builds) =
            self.collect_probe_roles(*left, pipelines, dependencies)?;
        let source = self.attach_hash_join_runtime_filters(source, &transforms, handle, spec);
        let probe_source_handles = source.clone();
        transforms.push(hash_join_probe_transform(handle, spec));
        transforms.extend(consumer_transforms.iter().cloned());
        let branch_sink = sink.clone();
        let pushed = self.push_pipeline(
            source,
            transforms,
            branch_sink,
            branch_sharing,
            output.clone(),
            pipelines,
            dependencies,
        )?;
        let consumer = pushed.entry;
        self.add_source_handle_dependencies(&probe_source_handles, consumer, dependencies)?;
        for pending in &pending_builds {
            self.handles.add_consumer(pending.handle, consumer)?;
        }
        self.handles.add_consumer(handle, consumer)?;

        dependencies.extend(
            pending_builds
                .into_iter()
                .map(|pending| PipelineDependency {
                    producer: pending.producer,
                    consumer,
                    kind: DependencyKind::BuildBeforeProbe,
                }),
        );
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::BuildBeforeProbe,
        });

        let replay = {
            // Non-forced hash joins can still switch to external mode during
            // build finish under memory pressure. Keep the replay fence in the
            // graph for every hash join; the source exits immediately when the
            // handle stayed in-memory.
            let replay_source = SourceSpec::HashJoinSpillReplay(HashJoinSpillReplaySourceSpec {
                handle,
                join_type: spec.join_type,
                conditions: spec.conditions.clone(),
                probe_types: self.plan.node(*left).output.types.clone(),
                build_payload_types: spec.right_output_types.clone(),
                left_projection: spec.left_projection.clone(),
                right_projection: spec.right_projection.clone(),
                output_names: spec.output_names.clone(),
                output_types: spec.output_types.clone(),
            });
            let replay = self.push_pipeline(
                replay_source,
                consumer_transforms.to_vec(),
                sink.clone(),
                branch_sharing,
                output.clone(),
                pipelines,
                dependencies,
            )?;
            self.handles.add_consumer(handle, replay.entry)?;
            dependencies.push(PipelineDependency {
                producer: consumer,
                consumer: replay.entry,
                kind: DependencyKind::ProbeBeforeSpillReplay,
            });
            Some(replay)
        };

        if needs_unmatched {
            let replay = replay.ok_or_else(|| {
                paro_error::internal("right/full hash join unmatched source requires replay fence")
            })?;
            let source = SourceSpec::HashJoinUnmatched(HashJoinUnmatchedSourceSpec {
                handle,
                join_type: spec.join_type,
                left_output_types: spec.left_output_types.clone(),
                right_projection: spec.right_projection.clone(),
                output_names: spec.output_names.clone(),
                output_types: spec.output_types.clone(),
            });
            let unmatched = self.push_pipeline(
                source,
                consumer_transforms,
                sink,
                branch_sharing,
                output,
                pipelines,
                dependencies,
            )?;
            self.handles.add_consumer(handle, unmatched.entry)?;
            dependencies.push(PipelineDependency {
                producer: replay.entry,
                consumer: unmatched.entry,
                kind: DependencyKind::FinalizeBeforeEmit,
            });
            return Ok(unmatched.tail);
        }

        Ok(replay.map_or(pushed.tail, |replay| replay.tail))
    }

    pub(crate) fn collect_probe_roles(
        &mut self,
        root: PhysicalPlanNodeId,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<(SourceSpec, Vec<TransformSpec>, Vec<PendingProbeBuild>)> {
        let node = self.plan.node(root);
        match &node.kind {
            PhysicalNodeKind::HashJoin(spec) => {
                if needs_hash_join_unmatched_source(spec.join_type) || spec.force_external {
                    return self.collect_probe_roles_source_fallback(root, pipelines, dependencies);
                }

                let spec = spec.clone();
                let children = self.plan.child_ids(&node.children);
                let [left, right] = children else {
                    return Err(paro_error::internal(format!(
                        "{} expected exactly two join children, got {}",
                        node.label.display_name,
                        children.len()
                    )));
                };

                let join_output =
                    RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec());
                let handle = self.handles.register(
                    BreakerHandleKind::HashJoinBuild,
                    join_output,
                    Default::default(),
                );
                let producer = self.lower_subtree_to_sink(
                    *right,
                    SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                        handle,
                        join_type: spec.join_type,
                        conditions: spec.conditions.clone(),
                        build_projection: spec.right_projection.clone(),
                        build_payload_types: spec.right_output_types.clone(),
                        required: Default::default(),
                        force_external: false,
                    }),
                    SinkSharing::Exclusive,
                    self.plan.node(*right).output.clone(),
                    pipelines,
                    dependencies,
                )?;
                self.handles.set_producer(handle, producer)?;

                let (source, mut transforms, mut pending_builds) =
                    self.collect_probe_roles(*left, pipelines, dependencies)?;
                let source =
                    self.attach_hash_join_runtime_filters(source, &transforms, handle, &spec);
                transforms.push(hash_join_probe_transform(handle, &spec));
                pending_builds.push(PendingProbeBuild { producer, handle });
                Ok((source, transforms, pending_builds))
            }
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                if needs_nlj_unmatched_source(spec.join_type) {
                    return self.collect_probe_roles_source_fallback(root, pipelines, dependencies);
                }
                let spec = spec.clone();
                let children = self.plan.child_ids(&node.children);
                let [left, right] = children else {
                    return Err(paro_error::internal(format!(
                        "{} expected exactly two NLJ children, got {}",
                        node.label.display_name,
                        children.len()
                    )));
                };

                let (producer, handle) =
                    self.lower_nlj_build(*right, &spec, pipelines, dependencies)?;

                let (source, mut transforms, mut pending_builds) =
                    self.collect_probe_roles(*left, pipelines, dependencies)?;
                transforms.push(nlj_probe_transform(handle, &spec));
                pending_builds.push(PendingProbeBuild { producer, handle });
                Ok((source, transforms, pending_builds))
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                if needs_nlj_unmatched_source(spec.join_type) {
                    return self.collect_probe_roles_source_fallback(root, pipelines, dependencies);
                }
                let children = self.plan.child_ids(&node.children);
                let [left, right] = children else {
                    return Err(paro_error::internal(format!(
                        "{} expected exactly two sort-range join children, got {}",
                        node.label.display_name,
                        children.len()
                    )));
                };

                let (producer, handle) =
                    self.lower_sort_range_build(*right, spec, pipelines, dependencies)?;

                let (source, mut transforms, mut pending_builds) =
                    self.collect_probe_roles(*left, pipelines, dependencies)?;
                transforms.push(sort_range_probe_transform(handle, spec));
                pending_builds.push(PendingProbeBuild { producer, handle });
                Ok((source, transforms, pending_builds))
            }
            PhysicalNodeKind::ClassicIeJoin(_) => {
                self.collect_probe_roles_source_fallback(root, pipelines, dependencies)
            }
            PhysicalNodeKind::CrossProduct(spec) => {
                let spec = spec.clone();
                let children = self.plan.child_ids(&node.children);
                let [left, right] = children else {
                    return Err(paro_error::internal(format!(
                        "{} expected exactly two cross product children, got {}",
                        node.label.display_name,
                        children.len()
                    )));
                };

                let (producer, handle) =
                    self.lower_cross_product_build(*right, &spec, pipelines, dependencies)?;

                let (source, mut transforms, mut pending_builds) =
                    self.collect_probe_roles(*left, pipelines, dependencies)?;
                transforms.push(cross_product_probe_transform(handle, &spec));
                pending_builds.push(PendingProbeBuild { producer, handle });
                Ok((source, transforms, pending_builds))
            }
            PhysicalNodeKind::ExternalTable(_) => {
                self.collect_probe_roles_source_fallback(root, pipelines, dependencies)
            }
            _ => {
                // A probe chain can only inline streaming operators and joins.  Other
                // breaker roots (for example a grouped aggregate on the left side of
                // a cross product) must first be completed and exposed through a
                // materialized source.  Falling through to collect_linear_roles would
                // otherwise try to execute the blocking operator as a transform.
                if self.breaker_dispatch_for_root(root).is_some() {
                    return self.collect_probe_roles_source_fallback(root, pipelines, dependencies);
                }
                if let Some(tail) = self.collect_tail_to_breaker(root, |kind| {
                    matches!(
                        kind,
                        PhysicalNodeKind::HashJoin(_)
                            | PhysicalNodeKind::NestedLoopJoin(_)
                            | PhysicalNodeKind::SortRangeJoin(_)
                            | PhysicalNodeKind::ClassicIeJoin(_)
                            | PhysicalNodeKind::CrossProduct(_)
                            | PhysicalNodeKind::ExternalTable(_)
                    )
                })? {
                    let (source, mut transforms, pending_builds) =
                        self.collect_probe_roles(tail.breaker, pipelines, dependencies)?;
                    transforms.extend(tail.transforms);
                    return Ok((source, transforms, pending_builds));
                }
                let (source, transforms) = self.collect_linear_roles(root)?;
                Ok((source, transforms, Vec::new()))
            }
        }
    }

    pub(crate) fn attach_hash_join_runtime_filters(
        &self,
        mut source: SourceSpec,
        transforms: &[TransformSpec],
        handle: BreakerHandleId,
        spec: &HashJoinSpec,
    ) -> SourceSpec {
        if !can_push_hash_join_runtime_filter(spec.join_type) || !transforms.is_empty() {
            return source;
        }
        let SourceSpec::Rowset(rowset) = &mut source else {
            return source;
        };
        for (build_key_index, condition) in spec.conditions.iter().enumerate() {
            if condition.comparison != JoinComparisonType::Equal {
                continue;
            }
            let Expression::Reference(reference) = &condition.left else {
                continue;
            };
            let Some(&probe_column_id) = rowset.scan.column_ids.get(reference.index) else {
                continue;
            };
            let Ok(probe_column_id) = u32::try_from(probe_column_id) else {
                continue;
            };
            rowset.add_dynamic_runtime_filter(RowsetDynamicRuntimeFilterSpec {
                handle,
                build_key_index,
                probe_column_id,
            });
        }
        source
    }

    pub(crate) fn collect_probe_roles_source_fallback(
        &mut self,
        root: PhysicalPlanNodeId,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<(SourceSpec, Vec<TransformSpec>, Vec<PendingProbeBuild>)> {
        let output = self.plan.node(root).output.clone();
        let handle = self.handles.register(
            BreakerHandleKind::Materialized,
            output.clone(),
            Default::default(),
        );
        let producer = self.lower_subtree_to_sink(
            root,
            SinkSpec::Materialize(MaterializeSinkSpec {
                handle,
                required: Default::default(),
            }),
            SinkSharing::Exclusive,
            output,
            pipelines,
            dependencies,
        )?;
        self.handles.set_producer(handle, producer)?;
        let source = SourceSpec::Materialized(MaterializedSourceSpec { handle });
        Ok((
            source,
            Vec::new(),
            vec![PendingProbeBuild { producer, handle }],
        ))
    }
}

fn can_push_hash_join_runtime_filter(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Inner | JoinType::Semi)
}
