// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    pub(crate) fn collect_linear_roles(
        &mut self,
        root: PhysicalPlanNodeId,
    ) -> Result<(SourceSpec, Vec<TransformSpec>, PipelineOperatorLineage)> {
        let mut current = root;
        let mut transforms = Vec::new();
        let mut transform_lineage = Vec::new();
        loop {
            let node = self.plan.node(current);
            match &node.kind {
                PhysicalNodeKind::RowsetScan(spec) => {
                    let mut source = RowsetSourceSpec::new(spec.clone());
                    self.attach_owned_hash_join_runtime_filters(current, &mut source)?;
                    return Ok(self.finish_linear_roles(
                        SourceSpec::Rowset(source),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::Values(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::Values(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::DummyScan(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::Dummy(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::EmptyResult(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::Empty(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::ChunkScan(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::Chunk(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::ExpressionScan(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::Expression(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::TableFunctionScan(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::TableFunction(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::VectorSearch(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::VectorSearch(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::SparseVectorSearch(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::SparseVectorSearch(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::FullTextSearch(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::FullTextSearch(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::AdaptiveSearch(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::AdaptiveSearch(spec.clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::GraphScan(spec) => {
                    return Ok(self.finish_linear_roles(
                        SourceSpec::GraphScan(spec.as_ref().clone()),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::CteScan(spec) => {
                    if let Some(handle) = self.recursive_cte_handles.get(&spec.cte_index).copied() {
                        return Ok(self.finish_linear_roles(
                            SourceSpec::RecursiveTableScan(RecursiveTableScanSourceSpec { handle }),
                            transforms,
                            transform_lineage,
                            node.label.logical_plan_node,
                        ));
                    }
                    let handle = *self.cte_handles.get(&spec.cte_index).ok_or_else(|| {
                        paro_error::internal(format!(
                            "CTE scan for index {} was lowered outside its materialized CTE scope",
                            spec.cte_index
                        ))
                    })?;
                    return Ok(self.finish_linear_roles(
                        SourceSpec::CteScan(CteScanSourceSpec { handle }),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::DelimScan(spec) => {
                    let handle = match spec.target {
                        DelimScanTarget::Values { table_index } => *self
                            .delim_value_handles
                            .get(&table_index)
                            .ok_or_else(|| {
                                paro_error::internal(format!(
                                    "Delim scan for table index {table_index} was lowered outside its correlated region"
                                ))
                            })?,
                        DelimScanTarget::CachedOuter => {
                            *self.cached_outer_handles.last().ok_or_else(|| {
                                paro_error::internal(
                                    "cached outer delim scan was lowered outside its correlated region",
                                )
                            })?
                        }
                    };
                    return Ok(self.finish_linear_roles(
                        SourceSpec::DelimScan(DelimScanSourceSpec { handle }),
                        transforms,
                        transform_lineage,
                        node.label.logical_plan_node,
                    ));
                }
                PhysicalNodeKind::Filter(spec) => {
                    transforms.push(TransformSpec::Filter(spec.clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Project(spec) => {
                    transforms.push(TransformSpec::Project(spec.clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Limit(spec) => {
                    transforms.push(TransformSpec::Limit(spec.as_ref().clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::TopN(spec) => {
                    ensure_streaming_topn_supported(spec)?;
                    transforms.push(TransformSpec::StreamingTopN(spec.clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Sort(_) => {
                    return Err(paro_error::not_implemented(
                        "blocking sort lowering is only supported when the sort is the pipeline root",
                    ));
                }
                PhysicalNodeKind::MutationInputSpool(_) => {
                    return Err(paro_error::internal(
                        "mutation input spool must lower as a materialization boundary",
                    ));
                }
                PhysicalNodeKind::SetOperation(_) => {
                    return Err(paro_error::not_implemented(
                        "set-operation lowering is only supported when the set operation is the pipeline breaker root",
                    ));
                }
                PhysicalNodeKind::HashJoin(_) => {
                    return Err(paro_error::not_implemented(
                        "hash join lowering is only supported when the join is a pipeline breaker root",
                    ));
                }
                PhysicalNodeKind::CrossProduct(_) => {
                    return Err(paro_error::not_implemented(
                        "cross product lowering is only supported when the join is a pipeline breaker root",
                    ));
                }
                PhysicalNodeKind::Aggregate(_) => {
                    return Err(paro_error::internal(
                        "aggregate must be lowered through its build/combine/emit breaker",
                    ));
                }
                PhysicalNodeKind::PartitionAggregateWindow(_) => {
                    return Err(paro_error::internal(
                        "partition aggregate window must lower through its build/finalize/emit breaker",
                    ));
                }
                PhysicalNodeKind::Window(spec) => {
                    if !is_streaming_window_supported(spec) {
                        return Err(paro_error::not_implemented(
                            "blocking window lowering requires typed breaker execution migration",
                        ));
                    }
                    transforms.push(TransformSpec::StreamingWindow(spec.clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::GraphExpand(spec) => {
                    transforms.push(TransformSpec::GraphExpand(spec.as_ref().clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::RowFetch(spec) => {
                    transforms.push(TransformSpec::RowFetch(spec.clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::GraphProject(spec) => {
                    transforms.push(TransformSpec::GraphProject(spec.clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::GraphShortestPath(spec) => {
                    transforms.push(TransformSpec::GraphShortestPath(spec.as_ref().clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::ExternalProject(spec) => {
                    transforms.push(TransformSpec::ExternalProject(spec.clone()));
                    transform_lineage.push(Some(node.label.logical_plan_node));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::NestedLoopJoin(_)
                | PhysicalNodeKind::SortRangeJoin(_)
                | PhysicalNodeKind::ClassicIeJoin(_) => {
                    return Err(paro_error::internal(
                        "nested loop / sort-range / classic IE join must be lowered as breaker, not linear role",
                    ));
                }
                PhysicalNodeKind::Insert(_)
                | PhysicalNodeKind::Update(_)
                | PhysicalNodeKind::Delete(_)
                | PhysicalNodeKind::CopyToFile(_) => {
                    return Err(paro_error::not_supported(
                        "terminal physical nodes only lower at pipeline root",
                    ));
                }
                PhysicalNodeKind::ExternalTable(_) => {
                    return Err(paro_error::internal(
                        "external table must be lowered as breaker, not linear role",
                    ));
                }
                PhysicalNodeKind::MaterializedCte(_) => {
                    return Err(paro_error::not_supported(
                        "materialized CTE wrapper only lowers at a subtree boundary",
                    ));
                }
                PhysicalNodeKind::RecursiveCte(_) => {
                    return Err(paro_error::not_supported(
                        "recursive CTE control region only lowers at a subtree boundary",
                    ));
                }
                PhysicalNodeKind::DelimJoin(_) => {
                    return Err(paro_error::not_supported(
                        "delim join control region only lowers at a subtree boundary",
                    ));
                }
                PhysicalNodeKind::Utility(_) => {
                    return Err(paro_error::not_supported(
                        "utility physical nodes do not lower to data pipelines",
                    ));
                }
            }
        }
    }

    fn finish_linear_roles(
        &self,
        source: SourceSpec,
        mut transforms: Vec<TransformSpec>,
        mut transform_lineage: Vec<Option<paro_planner::plan::PlanNodeId>>,
        source_node: paro_planner::plan::PlanNodeId,
    ) -> (SourceSpec, Vec<TransformSpec>, PipelineOperatorLineage) {
        transforms.reverse();
        transform_lineage.reverse();
        (
            source,
            transforms,
            PipelineOperatorLineage {
                source: Some(source_node),
                transforms: transform_lineage.into_boxed_slice(),
                sink: None,
            },
        )
    }

    pub(crate) fn only_child(&self, node_id: PhysicalPlanNodeId) -> Result<PhysicalPlanNodeId> {
        let node = self.plan.node(node_id);
        let children = self.plan.child_ids(&node.children);
        match children {
            [child] => Ok(*child),
            _ => Err(paro_error::internal(format!(
                "{} expected exactly one child, got {}",
                node.label.display_name,
                children.len()
            ))),
        }
    }

    pub(crate) fn collect_delim_scan_table_indexes(
        &self,
        root: PhysicalPlanNodeId,
    ) -> Result<Vec<usize>> {
        let mut indexes = Vec::new();
        self.collect_delim_scan_table_indexes_inner(root, &mut indexes)?;
        indexes.sort_unstable();
        indexes.dedup();
        Ok(indexes)
    }

    pub(crate) fn collect_delim_scan_table_indexes_inner(
        &self,
        root: PhysicalPlanNodeId,
        indexes: &mut Vec<usize>,
    ) -> Result<()> {
        let node = self.plan.node(root);
        if let PhysicalNodeKind::DelimScan(spec) = &node.kind {
            if let DelimScanTarget::Values { table_index } = spec.target {
                indexes.push(table_index);
            }
        }
        for child in self.plan.child_ids(&node.children) {
            self.collect_delim_scan_table_indexes_inner(*child, indexes)?;
        }
        Ok(())
    }
}
