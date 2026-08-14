// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::pipeline::graph::NljUnmatchedSourceSpec;

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
                    kind: pending.kind,
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
        let exact_single_row = nlj_scalar_filter_can_replace_probe(
            self.plan,
            *left,
            *right,
            spec,
            &consumer_transforms,
            &sink,
        );
        let (source, scalar_filter_replaces_probe) = self.attach_nlj_scalar_runtime_filter(
            source,
            &transforms,
            handle,
            spec,
            exact_single_row,
        );
        let source_handles = source.clone();
        if !scalar_filter_replaces_probe {
            transforms.push(nlj_probe_transform(handle, spec));
        }
        transforms.extend(consumer_transforms.iter().cloned());

        let pipeline_output = if scalar_filter_replaces_probe {
            self.plan.node(*left).output.clone()
        } else {
            output.clone()
        };
        let pushed = self.push_pipeline(
            source,
            transforms,
            sink.clone(),
            branch_sharing,
            pipeline_output,
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
                    kind: pending.kind,
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
                    kind: pending.kind,
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
            join_output.clone(),
            Default::default(),
        );

        // Probe, spill replay, and unmatched-build output are disjoint branches of one logical
        // join. Stateful downstream operators must observe their union once. A TopN can consume
        // that union directly through its shared bounded heap; other stateful transforms retain
        // the general materialized-union path.
        let topn_merge = hash_join_topn_merge(&consumer_transforms).map(|(index, spec)| {
            let input = consumer_transforms[..index]
                .iter()
                .fold(join_output.clone(), |row_type, transform| {
                    transform.output_row_type(&row_type)
                });
            let handle = self.handles.register(
                BreakerHandleKind::TopN,
                RowType::new(spec.output_names.to_vec(), spec.output_types.to_vec()),
                Default::default(),
            );
            (index, handle, spec.clone(), input)
        });
        let merge_handle = (topn_merge.is_none()
            && hash_join_requires_merge_barrier(&consumer_transforms))
        .then(|| {
            self.handles.register(
                BreakerHandleKind::Materialized,
                join_output.clone(),
                Default::default(),
            )
        });
        let branch_transforms = if let Some((index, ..)) = topn_merge.as_ref() {
            consumer_transforms[..*index].to_vec()
        } else if merge_handle.is_some() {
            Vec::new()
        } else {
            consumer_transforms.clone()
        };
        let (branch_sink, branch_sharing, branch_output) =
            if let Some((_, handle, spec, input)) = topn_merge.as_ref() {
                (
                    SinkSpec::TopNBuild(TopNBuildSinkSpec {
                        handle: *handle,
                        spec: spec.clone(),
                        required: Default::default(),
                    }),
                    SinkSharing::Shared(self.next_shared_sink()),
                    input.clone(),
                )
            } else if let Some(merge_handle) = merge_handle {
                (
                    SinkSpec::Materialize(MaterializeSinkSpec {
                        handle: merge_handle,
                        required: Default::default(),
                    }),
                    SinkSharing::Shared(self.next_shared_sink()),
                    join_output.clone(),
                )
            } else {
                let downstream_is_client = matches!(sink, SinkSpec::ClientResult(_))
                    && matches!(sink_sharing, SinkSharing::Exclusive);
                let sharing = if downstream_is_client {
                    SinkSharing::Exclusive
                } else {
                    match sink_sharing {
                        SinkSharing::Exclusive => SinkSharing::Shared(self.next_shared_sink()),
                        shared @ SinkSharing::Shared(_) => shared,
                    }
                };
                (sink.clone(), sharing, output.clone())
            };

        let producer = self.lower_subtree_to_sink(
            *right,
            SinkSpec::HashJoinBuild(HashJoinBuildSinkSpec {
                handle,
                join_type: spec.join_type,
                build_keys_unique: spec.build_keys_unique,
                build_time_integer_index: spec.build_time_integer_index.clone(),
                key_conditions: spec.key_conditions.clone(),
                residual_conditions: spec.build_residual_conditions.clone(),
                build_projection: spec.build_input_projection.clone(),
                build_payload_types: spec.build_payload_types.clone(),
                build_output_count: spec.build_output_count,
                grouped_reduction_channels: spec
                    .reduction_cascade
                    .as_ref()
                    .and_then(|cascade| cascade.grouped_extrema.as_ref())
                    .map(|grouped| grouped.channels.len()),
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
        transforms.extend(branch_transforms.iter().cloned());
        let pushed = self.push_pipeline(
            source,
            transforms,
            branch_sink.clone(),
            branch_sharing,
            branch_output.clone(),
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
                    kind: pending.kind,
                }),
        );
        dependencies.push(PipelineDependency {
            producer,
            consumer,
            kind: DependencyKind::BuildBeforeProbe,
        });

        // Non-forced hash joins can still switch to external mode during build
        // finish under memory pressure. Keep the replay fence in the graph for
        // every hash join; the source exits immediately when the handle stayed
        // in-memory.
        let replay_source = SourceSpec::HashJoinSpillReplay(HashJoinSpillReplaySourceSpec {
            handle,
            join_type: spec.join_type,
            anti_join_mode: spec.anti_join_mode,
            key_conditions: spec.key_conditions.clone(),
            build_residual_conditions: spec.build_residual_conditions.clone(),
            probe_residual_count: spec.probe_residual_count,
            probe_types: self.plan.node(*left).output.types.clone(),
            build_payload_types: spec.build_payload_types.clone(),
            build_output_count: spec.build_output_count,
            left_projection: spec.left_projection.clone(),
            output_names: spec.output_names.clone(),
            output_types: spec.output_types.clone(),
            reduction_cascade: spec.reduction_cascade.clone(),
        });
        let replay = self.push_pipeline(
            replay_source,
            branch_transforms.clone(),
            branch_sink.clone(),
            branch_sharing,
            branch_output.clone(),
            pipelines,
            dependencies,
        )?;
        self.handles.add_consumer(handle, replay.entry)?;
        dependencies.push(PipelineDependency {
            producer: pushed.tail,
            consumer: replay.entry,
            kind: DependencyKind::ProbeBeforeSpillReplay,
        });

        let mut last_branch = replay.tail;

        if needs_unmatched {
            let source = SourceSpec::HashJoinUnmatched(HashJoinUnmatchedSourceSpec {
                handle,
                join_type: spec.join_type,
                left_output_types: spec.left_output_types.clone(),
                output_names: spec.output_names.clone(),
                output_types: spec.output_types.clone(),
                reduction_cascade: spec.reduction_cascade.clone(),
            });
            let unmatched = self.push_pipeline(
                source,
                branch_transforms,
                branch_sink,
                branch_sharing,
                branch_output,
                pipelines,
                dependencies,
            )?;
            self.handles.add_consumer(handle, unmatched.entry)?;
            dependencies.push(PipelineDependency {
                producer: replay.tail,
                consumer: unmatched.entry,
                kind: DependencyKind::FinalizeBeforeEmit,
            });
            last_branch = unmatched.tail;
        }

        if let Some((index, topn_handle, spec, _)) = topn_merge {
            self.handles.set_producer(topn_handle, pushed.tail)?;
            let merged = self.push_pipeline(
                SourceSpec::TopNEmit(TopNEmitSourceSpec {
                    handle: topn_handle,
                    spec,
                }),
                consumer_transforms[index + 1..].to_vec(),
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            )?;
            self.handles.add_consumer(topn_handle, merged.entry)?;
            dependencies.push(PipelineDependency {
                producer: last_branch,
                consumer: merged.entry,
                kind: DependencyKind::FinalizeBeforeEmit,
            });
            return Ok(merged.tail);
        }

        let Some(merge_handle) = merge_handle else {
            return Ok(last_branch);
        };
        // A handle records one canonical producer for ownership/diagnostics, while
        // the shared sink also accepts replay and unmatched writers. The explicit
        // probe -> replay -> unmatched -> merged dependency chain above is the
        // execution-order guarantee; the merged consumer waits on `last_branch`.
        self.handles.set_producer(merge_handle, pushed.tail)?;
        let merged = self.push_pipeline(
            SourceSpec::Materialized(MaterializedSourceSpec {
                handle: merge_handle,
            }),
            consumer_transforms,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )?;
        self.handles.add_consumer(merge_handle, merged.entry)?;
        dependencies.push(PipelineDependency {
            producer: last_branch,
            consumer: merged.entry,
            kind: DependencyKind::FinalizeBeforeEmit,
        });
        Ok(merged.tail)
    }

    pub(crate) fn collect_probe_roles(
        &mut self,
        root: PhysicalPlanNodeId,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<(SourceSpec, Vec<TransformSpec>, Vec<PendingProbeDependency>)> {
        if Self::is_emit_breaker(&self.plan.node(root).kind) {
            let breaker = self.breaker_dispatch_for_root(root).ok_or_else(|| {
                paro_error::internal("emit breaker did not produce a lowering dispatch")
            })?;
            if let Some(probe_source) =
                self.lower_breaker_to_probe_source(root, breaker, pipelines, dependencies)?
            {
                return Ok((probe_source.source, Vec::new(), probe_source.dependencies));
            }
        }
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
                        build_keys_unique: spec.build_keys_unique,
                        build_time_integer_index: spec.build_time_integer_index.clone(),
                        key_conditions: spec.key_conditions.clone(),
                        residual_conditions: spec.build_residual_conditions.clone(),
                        build_projection: spec.build_input_projection.clone(),
                        build_payload_types: spec.build_payload_types.clone(),
                        build_output_count: spec.build_output_count,
                        grouped_reduction_channels: spec
                            .reduction_cascade
                            .as_ref()
                            .and_then(|cascade| cascade.grouped_extrema.as_ref())
                            .map(|grouped| grouped.channels.len()),
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
                pending_builds.push(PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::BuildBeforeProbe,
                });
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
                let (source, scalar_filter_replaces_probe) = self.attach_nlj_scalar_runtime_filter(
                    source,
                    &transforms,
                    handle,
                    &spec,
                    false,
                );
                debug_assert!(!scalar_filter_replaces_probe);
                if !scalar_filter_replaces_probe {
                    transforms.push(nlj_probe_transform(handle, &spec));
                }
                pending_builds.push(PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::BuildBeforeProbe,
                });
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
                pending_builds.push(PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::BuildBeforeProbe,
                });
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
                pending_builds.push(PendingProbeDependency {
                    producer,
                    handle,
                    kind: DependencyKind::BuildBeforeProbe,
                });
                Ok((source, transforms, pending_builds))
            }
            PhysicalNodeKind::ExternalTable(_) => {
                self.collect_probe_roles_source_fallback(root, pipelines, dependencies)
            }
            PhysicalNodeKind::MaterializedCte(_)
            | PhysicalNodeKind::RecursiveCte(_)
            | PhysicalNodeKind::DelimJoin(_) => {
                // Control regions are complete subtree producers, never
                // streaming transforms. Materialize their output as a probe
                // source so join orientation remains a cost decision instead
                // of being constrained by which child owns the region.
                self.collect_probe_roles_source_fallback(root, pipelines, dependencies)
            }
            _ => {
                // A probe chain can only inline streaming operators and joins.  Other
                // breaker roots (for example a grouped aggregate on the left side of
                // a cross product) must first be completed and exposed through a
                // materialized source.  Falling through to collect_linear_roles would
                // otherwise try to execute the blocking operator as a transform.
                if Self::is_tail_breaker(&node.kind) {
                    return self.collect_probe_roles_source_fallback(root, pipelines, dependencies);
                }
                if let Some(tail) = self.collect_tail_to_breaker(root, |kind| {
                    Self::is_tail_breaker(kind)
                        || matches!(kind, PhysicalNodeKind::MaterializedCte(_))
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
}

fn nlj_scalar_filter_can_replace_probe(
    plan: &crate::physical::PhysicalPlan,
    left: PhysicalPlanNodeId,
    right: PhysicalPlanNodeId,
    spec: &NestedLoopJoinSpec,
    consumer_transforms: &[TransformSpec],
    sink: &SinkSpec,
) -> bool {
    let left_output = &plan.node(left).output;
    spec.join_type == JoinType::Inner
        && spec.arbitrary_condition.is_none()
        && spec.left_projection.len() == left_output.column_count()
        && spec
            .left_projection
            .iter()
            .copied()
            .enumerate()
            .all(|(output_idx, input_idx)| output_idx == input_idx)
        && plan.guarantees_exactly_one_row(right)
        && consumer_transforms.is_empty()
        && sink_consumes_only_prefix(sink, left_output.column_count())
}

fn sink_consumes_only_prefix(sink: &SinkSpec, prefix_columns: usize) -> bool {
    let SinkSpec::HashJoinBuild(spec) = sink else {
        return false;
    };
    spec.build_projection
        .iter()
        .all(|column_idx| *column_idx < prefix_columns)
        && spec
            .key_conditions
            .iter()
            .all(|condition| expression_references_only_prefix(&condition.right, prefix_columns))
        && spec
            .residual_conditions
            .iter()
            .all(|condition| expression_references_only_prefix(&condition.right, prefix_columns))
}

fn expression_references_only_prefix(expression: &Expression, prefix_columns: usize) -> bool {
    use paro_planner::expression::{ExpressionIterator, ExpressionVisitDecision};

    let mut within_prefix = true;
    ExpressionIterator::visit(expression, &mut |child| match child {
        Expression::Reference(reference) if reference.index >= prefix_columns => {
            within_prefix = false;
            ExpressionVisitDecision::SkipChildren
        }
        // Column references must have been resolved before pipeline lowering.
        // Treat an unresolved binding as an unsafe dependency, never as proof.
        Expression::ColumnRef(_) | Expression::Subquery(_) => {
            within_prefix = false;
            ExpressionVisitDecision::SkipChildren
        }
        _ => ExpressionVisitDecision::Descend,
    });
    within_prefix
}

fn hash_join_requires_merge_barrier(transforms: &[TransformSpec]) -> bool {
    transforms.iter().any(|transform| {
        matches!(
            transform,
            TransformSpec::Limit(_)
                | TransformSpec::StreamingTopN(_)
                | TransformSpec::StreamingWindow(_)
        )
    })
}

fn hash_join_topn_merge(transforms: &[TransformSpec]) -> Option<(usize, &TopNSpec)> {
    let (index, transform) = transforms
        .iter()
        .enumerate()
        .find(|(_, transform)| is_stateful_transform(transform))?;
    match transform {
        TransformSpec::StreamingTopN(spec) => Some((index, spec)),
        _ => None,
    }
}

fn is_stateful_transform(transform: &TransformSpec) -> bool {
    matches!(
        transform,
        TransformSpec::Limit(_)
            | TransformSpec::StreamingTopN(_)
            | TransformSpec::StreamingWindow(_)
    )
}
