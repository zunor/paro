// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::physical::specs::SetOperationSpec;

impl PhysicalPlanExtractor {
    pub(crate) fn lower_set_operation(
        &mut self,
        setop: &LogicalSetOperation<SelectedChild>,
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
                    relation_alias: None,
                    expressions: rows.into_boxed_slice(),
                    output_names: output_names.into_boxed_slice(),
                    output_types: setop.types.clone().into_boxed_slice(),
                };
                return Ok((PhysicalNodeKind::Values(spec), Vec::new()));
            }
        }

        let left = self.extract_node(setop.left.as_ref())?;
        let right = self.extract_node(setop.right.as_ref())?;
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
        cte: &LogicalMaterializedCte<SelectedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let producer = self.extract_node(cte.cte_query.as_ref())?;
        let consumer = self.extract_node(cte.child.as_ref())?;
        let spec = MaterializedCteSpec {
            cte_index: cte.cte_index,
            cte_name: cte.cte_name.clone(),
            materialized: cte.materialized,
            ref_count: cte.ref_count,
            column_names: cte.column_names.clone().into_boxed_slice(),
            column_types: cte.column_types.clone().into_boxed_slice(),
            spill_policy: self.ctx.spill_execution_policy(true),
        };
        Ok((
            PhysicalNodeKind::MaterializedCte(spec),
            vec![producer, consumer],
        ))
    }

    pub(crate) fn lower_recursive_cte(
        &mut self,
        cte: &LogicalRecursiveCte<SelectedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let anchor = self.extract_node(cte.anchor.as_ref())?;
        let recursive = self.extract_node(cte.recursive.as_ref())?;
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
            relation_alias: cte_ref.relation_alias.clone(),
            output_names: cte_ref.column_names.clone().into_boxed_slice(),
            output_types: cte_ref.column_types.clone().into_boxed_slice(),
        };
        (PhysicalNodeKind::CteScan(spec), Vec::new())
    }

    pub(crate) fn lower_explain(
        &mut self,
        explain: &LogicalExplain<SelectedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if explain.spec.mode == ExplainMode::Analyze {
            return self.reject_unimplemented(
                "EXPLAIN_ANALYZE",
                "EXPLAIN ANALYZE must be compiled as a top-level runtime wrapper",
            );
        }

        let mut child_extractor = PhysicalPlanExtractor::new(self.ctx.clone())
            .with_winner_contracts(self.winner_contracts.clone())
            .with_enforcer_contracts(self.enforcer_contracts.clone())
            .with_statement_write_contracts(self.statement_write_contracts.clone());
        if self.require_winner_contracts {
            child_extractor = child_extractor.requiring_winner_contracts();
        }
        let child_plan = child_extractor.extract_selected(explain.child.as_ref())?;
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
            relation_alias: None,
            expressions: rows.into_boxed_slice(),
            output_names: Box::new(["QUERY PLAN".to_string()]),
            output_types: Box::new([paro_common::types::LogicalType::Varchar]),
        };
        Ok((PhysicalNodeKind::Values(spec), Vec::new()))
    }

    pub(crate) fn lower_unsupported(
        &mut self,
        op: &LogicalOperator<SelectedChild>,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        self.reject_unimplemented(logical_name(op), "typed physical spec is not implemented")
    }

    pub(crate) fn reject_unimplemented<T>(
        &self,
        logical_name: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<T> {
        Err(paro_error::not_implemented(format!(
            "physical implementation for {} ({})",
            logical_name.into(),
            reason.into()
        )))
    }
}
