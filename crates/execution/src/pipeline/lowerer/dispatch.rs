// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Thin pipeline-lowering dispatch shell.

use super::*;

impl<'a> PipelineLowerer<'a> {
    pub(crate) fn lower_subtree_to_sink(
        &mut self,
        root: PhysicalPlanNodeId,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        output: RowType,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        match &self.plan.node(root).kind {
            PhysicalNodeKind::MaterializedCte(spec) => {
                return self.lower_materialized_cte_to_sink(
                    root,
                    spec.clone(),
                    sink,
                    sink_sharing,
                    output,
                    pipelines,
                    dependencies,
                );
            }
            PhysicalNodeKind::RecursiveCte(spec) => {
                return self.lower_recursive_cte_to_sink(
                    root,
                    spec.clone(),
                    Vec::new(),
                    sink,
                    sink_sharing,
                    output,
                    pipelines,
                    dependencies,
                );
            }
            PhysicalNodeKind::DelimJoin(spec) => {
                return self.lower_delim_join_to_sink(
                    root,
                    spec.clone(),
                    Vec::new(),
                    sink,
                    sink_sharing,
                    output,
                    pipelines,
                    dependencies,
                );
            }
            _ => {}
        }

        if let Some(breaker) = self.breaker_dispatch_for_root(root) {
            return self.dispatch_breaker_to_sink(
                root,
                breaker,
                Vec::new(),
                sink,
                sink_sharing,
                output,
                pipelines,
                dependencies,
            );
        }

        if let Some(tail) = self.collect_tail_to_breaker(root, Self::is_tail_breaker)? {
            return self.lower_tail_breaker_to_sink(
                tail,
                sink,
                sink_sharing,
                pipelines,
                dependencies,
            );
        }

        self.lower_linear_pipeline_to_sink(
            root,
            sink,
            sink_sharing,
            output,
            pipelines,
            dependencies,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_tail_breaker_to_sink(
        &mut self,
        tail: BreakerTail,
        sink: SinkSpec,
        sink_sharing: SinkSharing,
        pipelines: &mut Vec<PipelineSpec>,
        dependencies: &mut Vec<PipelineDependency>,
    ) -> Result<PipelineId> {
        match &self.plan.node(tail.breaker).kind {
            PhysicalNodeKind::RecursiveCte(spec) => {
                return self.lower_recursive_cte_to_sink(
                    tail.breaker,
                    spec.clone(),
                    tail.transforms,
                    sink,
                    sink_sharing,
                    tail.output,
                    pipelines,
                    dependencies,
                );
            }
            PhysicalNodeKind::DelimJoin(spec) => {
                return self.lower_delim_join_to_sink(
                    tail.breaker,
                    spec.clone(),
                    tail.transforms,
                    sink,
                    sink_sharing,
                    tail.output,
                    pipelines,
                    dependencies,
                );
            }
            _ => {}
        }

        let breaker = self.tail_breaker_dispatch(tail.breaker)?;
        self.dispatch_breaker_to_sink(
            tail.breaker,
            breaker,
            tail.transforms,
            sink,
            sink_sharing,
            tail.output,
            pipelines,
            dependencies,
        )
    }
}
