// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::physical::specs::{SearchFilterContract, SearchPredicateTemplate};

impl PhysicalPlanGenerator {
    pub(crate) fn lower_insert(
        &mut self,
        insert: &LogicalInsert,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(insert.child.as_ref())?;
        let spec = InsertSpec {
            table: insert.table.clone(),
            column_index_map: insert.column_index_map.clone().into_boxed_slice(),
            expected_types: insert.expected_types.clone().into_boxed_slice(),
            on_conflict: insert.on_conflict.clone(),
            copy_from_read_csv: is_read_csv_table_function(insert.child.as_ref()),
        };
        Ok((PhysicalNodeKind::Insert(spec), vec![child]))
    }

    pub(crate) fn lower_delete(
        &mut self,
        delete: &LogicalDelete,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(delete.child.as_ref())?;
        let child_width = self
            .arena
            .get(child)
            .map(|node| node.output.column_count())
            .unwrap_or(0);
        if child_width == 0 {
            return Ok((
                self.unsupported("DELETE", "child operator has no row id column"),
                vec![child],
            ));
        }
        let spec = DeleteSpec {
            table: delete.table.clone(),
            row_id_index: child_width - 1,
            is_full_table_delete: delete.is_full_table_delete,
        };
        Ok((PhysicalNodeKind::Delete(spec), vec![child]))
    }

    pub(crate) fn lower_update(
        &mut self,
        update: &LogicalUpdate,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(update.child.as_ref())?;
        let Some(child_node) = self.arena.get(child) else {
            return Err(paro_error::internal("UPDATE child node is missing"));
        };
        let child_types = child_node.output.types.clone();
        let child_cardinality = child_node.cardinality;
        if child_types.is_empty() {
            return Ok((
                self.unsupported("UPDATE", "child operator has no row id column"),
                vec![child],
            ));
        }
        if update.columns.len() != update.expressions.len() {
            return Ok((
                self.unsupported(
                    "UPDATE",
                    "target column count does not match expression count",
                ),
                vec![child],
            ));
        }

        let table_column_count = update.table.columns.len();
        if child_types.len() < table_column_count + 1 {
            return Ok((
                self.unsupported("UPDATE", "child output is missing full row or row id"),
                vec![child],
            ));
        }

        let mut assignment_positions = vec![None; table_column_count];
        for (expr_idx, &column_idx) in update.columns.iter().enumerate() {
            if column_idx >= table_column_count
                || assignment_positions[column_idx].replace(expr_idx).is_some()
            {
                return Ok((
                    self.unsupported("UPDATE", "invalid or duplicate target column"),
                    vec![child],
                ));
            }
        }

        let mut projection_exprs = Vec::with_capacity(table_column_count + 1);
        let mut output_names = Vec::with_capacity(table_column_count + 1);
        let mut output_types = Vec::with_capacity(table_column_count + 1);
        for table_col_idx in 0..table_column_count {
            let column = &update.table.columns[table_col_idx];
            if let Some(expr_idx) = assignment_positions[table_col_idx] {
                projection_exprs.push(update.expressions[expr_idx].clone());
            } else {
                projection_exprs.push(Expression::Reference(ReferenceExpression::new(
                    table_col_idx,
                    child_types[table_col_idx].clone(),
                )));
            }
            output_names.push(column.name.clone());
            output_types.push(column.logical_type.clone());
        }
        let scan_row_id_index = child_types.len() - 1;
        projection_exprs.push(Expression::Reference(ReferenceExpression::new(
            scan_row_id_index,
            child_types[scan_row_id_index].clone(),
        )));
        output_names.push("rowid".to_string());
        output_types.push(child_types[scan_row_id_index].clone());

        let project_id = self.push_node(
            PhysicalNodeKind::Project(ProjectSpec {
                expressions: projection_exprs.into_boxed_slice(),
                output_names: output_names.clone().into_boxed_slice(),
                visible_count: 0,
            }),
            RowType::with_identities(
                output_names.clone(),
                output_types,
                output_names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        if index == table_column_count {
                            ColumnIdentity::Internal
                        } else {
                            ColumnIdentity::visible(name.clone())
                        }
                    })
                    .collect(),
            ),
            vec![child],
            OperatorLabel::new(update.child.id, "UPDATE_PROJECT"),
            child_cardinality,
        );

        let spec = UpdateSpec {
            table: update.table.clone(),
            columns: update.columns.clone().into_boxed_slice(),
            row_id_index: table_column_count,
        };
        Ok((PhysicalNodeKind::Update(spec), vec![project_id]))
    }

    pub(crate) fn lower_copy_to(
        &mut self,
        copy: &LogicalCopyTo,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let child = self.generate_node(copy.child.as_ref())?;
        let spec = CopyToFileSpec {
            copy_function: copy.copy_function.clone(),
            bind_data: copy.bind_data.clone(),
            file_path: copy.file_path.clone(),
            per_thread_output: copy.options.per_thread_output,
            output_types: copy.types.clone().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::CopyToFile(spec), vec![child]))
    }

    pub(crate) fn lower_create_index(
        &mut self,
        create_index: &LogicalCreateIndex,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        if !create_index.info.index_type.supports_metadata_only_build() {
            return Ok((
                self.unsupported(
                    "CREATE_INDEX",
                    "runtime index backfill is not migrated to typed pipelines yet",
                ),
                Vec::new(),
            ));
        }

        let spec = CreateIndexUtilitySpec {
            table: create_index.table.clone(),
            info: create_index.info.clone(),
        };
        Ok((
            PhysicalNodeKind::Utility(UtilitySpec::CreateIndex(spec)),
            Vec::new(),
        ))
    }

    pub(crate) fn lower_fulltext_filter_scan(
        &mut self,
        scan: &LogicalFullTextFilterScan,
    ) -> Result<(PhysicalNodeKind, Vec<PhysicalPlanNodeId>)> {
        let candidate = match &scan.decision {
            SearchDecision::IndexScan { candidate, .. } => candidate,
            SearchDecision::Adaptive { candidates, .. } => {
                let Some(candidate) = candidates
                    .iter()
                    .find(|candidate| matches!(candidate.intent, SearchIntent::FullText(_)))
                else {
                    return Ok((
                        self.unsupported(
                            "FULL_TEXT_FILTER_SCAN",
                            "adaptive fulltext scan has no fulltext index candidate",
                        ),
                        Vec::new(),
                    ));
                };
                candidate
            }
        };
        let SearchIntent::FullText(intent) = &candidate.intent else {
            return Ok((
                self.unsupported(
                    "FULL_TEXT_FILTER_SCAN",
                    "planned search candidate is not fulltext",
                ),
                Vec::new(),
            ));
        };
        let table =
            scan.get.get_table().cloned().ok_or_else(|| {
                paro_error::internal("Get missing table reference for fulltext scan")
            })?;
        let (predicate_tree, mut residual) =
            predicate_builder::build_search_predicate_template(&scan.other_predicates, &scan.get)?;
        let (runtime_tree, mut runtime_residual) =
            predicate_builder::build_search_predicate_template(
                &scan.get.runtime_filter_expressions,
                &scan.get,
            )?;
        let predicate =
            SearchPredicateTemplate::and([predicate_tree, runtime_tree].into_iter().flatten());
        residual.append(&mut runtime_residual);
        residual.extend(scan.residual_predicates.clone());
        if !residual.is_empty() {
            return Ok((
                self.unsupported(
                    "FULL_TEXT_FILTER_SCAN",
                    "residual fulltext filters require a typed filter node above the search source",
                ),
                Vec::new(),
            ));
        }

        let filter_contract = SearchFilterContract::for_predicate(predicate.as_ref());
        let spec = FullTextSearchSpec {
            table,
            capability_token: candidate.token.clone(),
            column_id: intent.column_id as usize,
            query: intent.query.clone(),
            query_kind: intent.query_kind,
            query_stats: intent.query_stats,
            config: intent.config.clone(),
            score_mode: intent.score_mode,
            mode: SearchRequestMode::Filter,
            predicate,
            filter_contract,
            projected_columns: (0..scan.get.returned_types.len())
                .map(|output| {
                    scan.get.stored_column(output).ok_or_else(|| {
                        paro_error::internal(
                            "full-text scan cannot lower a derived output as a stored column",
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?
                .into_boxed_slice(),
            emit_score: false,
            output_names: scan.get.names.clone().into_boxed_slice(),
            output_types: scan.get.returned_types.clone().into_boxed_slice(),
        };
        Ok((PhysicalNodeKind::FullTextSearch(spec), Vec::new()))
    }
}
