// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::physical::specs::SetOperationSpec;

impl PhysicalPlanGenerator {
    pub(crate) fn lower_set_operation(
        &mut self,
        setop: &LogicalSetOperation,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if setop.setop_type == SetOpType::Union && setop.setop_all {
            if let Some(rows) = collect_union_all_row_literals(setop)? {
                let output_names = align_output_names(
                    setop.left.output_names(),
                    setop.types.len(),
                    "union output",
                )?;
                let spec = ValuesSpec {
                    table_index: setop.table_index,
                    expressions: rows.into_boxed_slice(),
                    output_names: output_names.into_boxed_slice(),
                    output_types: setop.types.clone().into_boxed_slice(),
                };
                return Ok((PhysicalNodeKind::Values(spec), Vec::new()));
            }
        }

        let left = self.generate_node(setop.left.as_ref())?;
        let right = self.generate_node(setop.right.as_ref())?;
        let output_names = align_output_names(
            setop.left.output_names(),
            setop.types.len(),
            "set operation output",
        )?;
        let spec = SetOperationSpec {
            table_index: setop.table_index,
            op: setop.setop_type,
            all: setop.setop_all,
            output_names: output_names.into_boxed_slice(),
            output_types: setop.types.clone().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::SetOperation(spec), vec![left, right]))
    }

    pub(crate) fn lower_materialized_cte(
        &mut self,
        cte: &LogicalMaterializedCte,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let producer = self.generate_node(cte.cte_query.as_ref())?;
        let consumer = self.generate_node(cte.child.as_ref())?;
        let spec = MaterializedCteSpec {
            cte_index: cte.cte_index,
            cte_name: cte.cte_name.clone(),
            materialized: cte.materialized,
            ref_count: cte.ref_count,
            column_names: cte.column_names.clone().into_boxed_slice(),
            column_types: cte.column_types.clone().into_boxed_slice(),
        };
        Ok((
            PhysicalNodeKind::MaterializedCte(spec),
            vec![producer, consumer],
        ))
    }

    pub(crate) fn lower_recursive_cte(
        &mut self,
        cte: &LogicalRecursiveCte,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let anchor = self.generate_node(cte.anchor.as_ref())?;
        let recursive = self.generate_node(cte.recursive.as_ref())?;
        let spec = RecursiveCteSpec {
            cte_index: cte.cte_index,
            cte_name: cte.cte_name.clone(),
            column_names: cte.column_names.clone().into_boxed_slice(),
            column_types: cte.column_types.clone().into_boxed_slice(),
            union_all: cte.union_all,
        };
        Ok((
            PhysicalNodeKind::RecursiveCte(spec),
            vec![anchor, recursive],
        ))
    }

    pub(crate) fn lower_cte_ref(
        &mut self,
        cte_ref: &LogicalCteRef,
    ) -> (PhysicalNodeKind, Vec<PhysicalPlanNodeId>) {
        let spec = CteScanSpec {
            cte_index: cte_ref.cte_index,
            table_index: cte_ref.table_index,
            output_names: cte_ref.column_names.clone().into_boxed_slice(),
            output_types: cte_ref.column_types.clone().into_boxed_slice(),
        };
        (PhysicalNodeKind::CteScan(spec), Vec::new())
    }

    pub(crate) fn lower_explain(
        &mut self,
        explain: &LogicalExplain,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if explain.spec.mode == ExplainMode::Analyze {
            return self.unsupported_preserving_children(
                "EXPLAIN_ANALYZE",
                "EXPLAIN ANALYZE must be compiled as a top-level runtime wrapper",
                &[explain.child.as_ref()],
            );
        }

        let mut child_generator = PhysicalPlanGenerator::new(self.ctx.clone());
        let child_plan = child_generator.generate(explain.child.as_ref())?;
        let rows = match explain.spec.format {
            ExplainFormat::Text => child_plan
                .format_explain_text_with_spec(&explain.spec)
                .lines()
                .map(explain_line_expression)
                .collect::<Vec<_>>(),
            ExplainFormat::Json => {
                vec![explain_line_expression(
                    child_plan.format_explain_json(explain.spec),
                )]
            }
        };

        let spec = ValuesSpec {
            table_index: 0,
            expressions: rows.into_boxed_slice(),
            output_names: Box::new(["QUERY PLAN".to_string()]),
            output_types: Box::new([paro_common::types::LogicalType::Varchar]),
        };
        Ok((PhysicalNodeKind::Values(spec), Vec::new()))
    }

    pub(crate) fn lower_unsupported(
        &mut self,
        op: &LogicalOperator,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let mut children = Vec::new();
        for child in op.children() {
            children.push(self.generate_node(child)?);
        }
        Ok((
            self.unsupported(logical_name(op), "typed physical spec is not migrated yet"),
            children,
        ))
    }

    pub(crate) fn unsupported(
        &self,
        logical_name: impl Into<String>,
        reason: impl Into<String>,
    ) -> PhysicalNodeKind {
        PhysicalNodeKind::Unsupported(UnsupportedSpec {
            logical_name: logical_name.into(),
            reason: reason.into(),
        })
    }

    pub(crate) fn unsupported_preserving_children(
        &mut self,
        logical_name: impl Into<String>,
        reason: impl Into<String>,
        logical_children: &[&LogicalPlan],
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let mut children = Vec::with_capacity(logical_children.len());
        for child in logical_children {
            children.push(self.generate_node(child)?);
        }
        Ok((self.unsupported(logical_name, reason), children))
    }

    pub fn ensure_fully_typed(plan: &PhysicalPlan) -> Result<()> {
        if let Some(node) = plan
            .nodes
            .iter()
            .find(|node| matches!(node.kind, PhysicalNodeKind::Unsupported(_)))
        {
            let detail = match &node.kind {
                PhysicalNodeKind::Unsupported(spec) => {
                    format!("{} (reason: {})", spec.logical_name, spec.reason)
                }
                _ => node.label.display_name.clone(),
            };
            return Err(paro_error::not_implemented(format!(
                "arena physical plan lowering for {}",
                detail
            )));
        }
        Ok(())
    }
}
