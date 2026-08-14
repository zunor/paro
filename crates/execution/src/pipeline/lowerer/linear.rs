// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<'a> PipelineLowerer<'a> {
    pub(crate) fn collect_linear_roles(
        &mut self,
        root: PhysicalPlanNodeId,
    ) -> Result<(SourceSpec, Vec<TransformSpec>)> {
        let mut current = root;
        let mut transforms = Vec::new();
        loop {
            let node = self.plan.node(current);
            match &node.kind {
                PhysicalNodeKind::RowsetScan(spec) => {
                    transforms.reverse();
                    return Ok((
                        SourceSpec::Rowset(RowsetSourceSpec::new(spec.clone())),
                        transforms,
                    ));
                }
                PhysicalNodeKind::Values(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::Values(spec.clone()), transforms));
                }
                PhysicalNodeKind::DummyScan(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::Dummy(spec.clone()), transforms));
                }
                PhysicalNodeKind::EmptyResult(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::Empty(spec.clone()), transforms));
                }
                PhysicalNodeKind::ChunkScan(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::Chunk(spec.clone()), transforms));
                }
                PhysicalNodeKind::ExpressionScan(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::Expression(spec.clone()), transforms));
                }
                PhysicalNodeKind::TableFunctionScan(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::TableFunction(spec.clone()), transforms));
                }
                PhysicalNodeKind::VectorSearch(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::VectorSearch(spec.clone()), transforms));
                }
                PhysicalNodeKind::SparseVectorSearch(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::SparseVectorSearch(spec.clone()), transforms));
                }
                PhysicalNodeKind::FullTextSearch(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::FullTextSearch(spec.clone()), transforms));
                }
                PhysicalNodeKind::AdaptiveSearch(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::AdaptiveSearch(spec.clone()), transforms));
                }
                PhysicalNodeKind::GraphScan(spec) => {
                    transforms.reverse();
                    return Ok((SourceSpec::GraphScan(spec.clone()), transforms));
                }
                PhysicalNodeKind::CteScan(spec) => {
                    if let Some(handle) = self.recursive_cte_handles.get(&spec.cte_index).copied() {
                        transforms.reverse();
                        return Ok((
                            SourceSpec::RecursiveTableScan(RecursiveTableScanSourceSpec { handle }),
                            transforms,
                        ));
                    }
                    let handle = *self.cte_handles.get(&spec.cte_index).ok_or_else(|| {
                        paro_error::internal(format!(
                            "CTE scan for index {} was lowered outside its materialized CTE scope",
                            spec.cte_index
                        ))
                    })?;
                    transforms.reverse();
                    return Ok((
                        SourceSpec::CteScan(CteScanSourceSpec { handle }),
                        transforms,
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
                    transforms.reverse();
                    return Ok((
                        SourceSpec::DelimScan(DelimScanSourceSpec { handle }),
                        transforms,
                    ));
                }
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
                PhysicalNodeKind::TopN(spec) => {
                    ensure_streaming_topn_supported(spec)?;
                    transforms.push(TransformSpec::StreamingTopN(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::Sort(_) => {
                    return Err(paro_error::not_implemented(
                        "blocking sort lowering is only supported when the sort is the pipeline root",
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
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::GraphExpand(spec) => {
                    transforms.push(TransformSpec::GraphExpand(spec.clone()));
                    current = self.only_child(current)?;
                }
                PhysicalNodeKind::RowFetchProject(spec) => {
                    transforms.push(TransformSpec::RowFetchProject(spec.clone()));
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
                PhysicalNodeKind::Unsupported(spec) => {
                    return Err(paro_error::not_implemented(format!(
                        "pipeline lowering for {} ({})",
                        spec.logical_name, spec.reason
                    )));
                }
                PhysicalNodeKind::Utility(_) => {
                    return Err(paro_error::not_supported(
                        "utility physical nodes do not lower to data pipelines",
                    ));
                }
            }
        }
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
