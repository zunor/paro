// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    pub(crate) fn collect_tail_to_breaker(
        &mut self,
        root: PhysicalPlanNodeId,
        mut is_breaker: impl FnMut(&PhysicalNodeKind) -> bool,
    ) -> Result<Option<BreakerTail>> {
        let mut current = root;
        let mut transforms = Vec::new();
        loop {
            let node = self.plan.node(current);
            match &node.kind {
                PhysicalNodeKind::Filter(spec) => {
                    transforms.push(TransformSpec::Filter(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Project(spec) => {
                    transforms.push(TransformSpec::Project(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Limit(spec) => {
                    transforms.push(TransformSpec::Limit(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Window(spec) if is_streaming_window_supported(spec) => {
                    let child = self.only_child(current)?;
                    if self.subtree_root_needs_post_join_fanout(child)? {
                        transforms.reverse();
                        return Ok(Some(BreakerTail {
                            breaker: current,
                            transforms,
                            output: self.plan.node(root).output.clone(),
                        }));
                    }
                    transforms.push(TransformSpec::StreamingWindow(spec.clone()));
                    current = child;
                }
                PhysicalNodeKind::GraphExpand(spec) => {
                    transforms.push(TransformSpec::GraphExpand(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::RowFetch(spec) => {
                    transforms.push(TransformSpec::RowFetch(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::GraphProject(spec) => {
                    transforms.push(TransformSpec::GraphProject(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::GraphShortestPath(spec) => {
                    transforms.push(TransformSpec::GraphShortestPath(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::ExternalProject(spec) => {
                    transforms.push(TransformSpec::ExternalProject(spec.clone()));
                    current = self.only_child(current)?;
                }
                kind if is_breaker(kind) && !transforms.is_empty() => {
                    transforms.reverse();
                    return Ok(Some(BreakerTail {
                        breaker: current,
                        transforms,
                        output: self.plan.node(root).output.clone(),
                    }));
                }
                _ => return Ok(None),
            }
        }
    }

    pub(crate) fn subtree_root_needs_post_join_fanout(
        &mut self,
        root: PhysicalPlanNodeId,
    ) -> Result<bool> {
        if let Some(result) = self.post_join_fanout_cache[root.index()] {
            return Ok(result);
        }

        let child = match &self.plan.node(root).kind {
            PhysicalNodeKind::HashJoin(_) => {
                let result = true;
                self.post_join_fanout_cache[root.index()] = Some(result);
                return Ok(result);
            }
            PhysicalNodeKind::NestedLoopJoin(spec) => {
                let result = needs_nlj_unmatched_source(spec.join_type);
                self.post_join_fanout_cache[root.index()] = Some(result);
                return Ok(result);
            }
            PhysicalNodeKind::SortRangeJoin(spec) => {
                let result = needs_nlj_unmatched_source(spec.join_type);
                self.post_join_fanout_cache[root.index()] = Some(result);
                return Ok(result);
            }
            PhysicalNodeKind::Filter(_)
            | PhysicalNodeKind::Project(_)
            | PhysicalNodeKind::Limit(_)
            | PhysicalNodeKind::GraphExpand(_)
            | PhysicalNodeKind::RowFetch(_)
            | PhysicalNodeKind::GraphProject(_)
            | PhysicalNodeKind::GraphShortestPath(_)
            | PhysicalNodeKind::ExternalProject(_) => Some(self.only_child(root)?),
            PhysicalNodeKind::Window(spec) if is_streaming_window_supported(spec) => {
                Some(self.only_child(root)?)
            }
            _ => None,
        };

        let result = match child {
            Some(child) => self.subtree_root_needs_post_join_fanout(child)?,
            None => false,
        };
        self.post_join_fanout_cache[root.index()] = Some(result);
        Ok(result)
    }
}
