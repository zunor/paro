// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Stable identities for planner operators, scalar payloads, and searches.

use super::*;
use paro_planner::physical::access_identity::*;

pub(super) fn binding_fingerprint(binding: ColumnBinding) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_u64(binding.table_index as u64);
    fingerprint.write_u64(binding.column_index as u64);
    fingerprint.finish()
}

pub(super) fn typed_binding_fingerprint(
    binding: ColumnBinding,
    type_domain: Fingerprint,
) -> Fingerprint {
    let mut fingerprint = StableFingerprintBuilder::default();
    fingerprint.write_fingerprint(binding_fingerprint(binding));
    fingerprint.write_fingerprint(type_domain);
    fingerprint.finish()
}

#[cfg(test)]
pub(super) fn query_operator_fingerprint(
    plan: &OwnedLogicalPlan,
    scalar_roots: &[ScalarExprId],
    scalars: &ScalarArena,
) -> Result<Fingerprint> {
    Ok(query_operator_identity(&plan.operator, scalar_roots, scalars)?.0)
}

/// Hash-bucket key plus exact canonical encoding for one relational shell.
/// The encoding is retained by the Memo payload and is the final equality
/// check; the 128-bit digest is only an index accelerator.
pub(super) fn query_operator_identity<Child>(
    operator: &LogicalOperator<Child>,
    scalar_roots: &[ScalarExprId],
    scalars: &ScalarArena,
) -> Result<(Fingerprint, Box<[u8]>)> {
    let mut fingerprint = StableFingerprintBuilder::recording();
    fingerprint.write_u64(operator_tag(operator.op_type()));
    let semantic_roots = crate::cascades::scalar_lowering::semantic_scalar_root_fingerprints(
        operator,
        scalar_roots,
        scalars,
    )?;
    fingerprint.write_u64(semantic_roots.len() as u64);
    for root in semantic_roots {
        fingerprint.write_fingerprint(root);
    }
    match operator {
        LogicalOperator::Get(get) => encode_get(&mut fingerprint, get),
        LogicalOperator::BoundReference(_) => {
            return Err(paro_error::internal(
                "transformation group hole crossed the Memo staging boundary",
            ));
        }
        LogicalOperator::Filter(_) => {}
        LogicalOperator::Projection(projection) => {
            fingerprint.write_u64(projection.visible_count as u64);
            encode_optional_string(&mut fingerprint, projection.visible_qualifier.as_deref());
        }
        LogicalOperator::RowFetch(fetch) => {
            fingerprint.write_u64(fetch.sources.len() as u64);
            for source in &fetch.sources {
                fingerprint.write_u64(source.table.object_id().raw());
                encode_usizes(&mut fingerprint, &source.needed_columns);
            }
        }
        LogicalOperator::ExternalProject(project) => {
            fingerprint.write_u64(project.expressions.len() as u64);
            for expression in &project.expressions {
                encode_external_call(&mut fingerprint, &expression.routine_meta);
            }
        }
        LogicalOperator::ExternalTable(table) => {
            encode_external_call(&mut fingerprint, &table.call);
            fingerprint.write_u64(table.lateral as u64);
            fingerprint.write_u64(table.parameterized as u64);
        }
        LogicalOperator::Limit(limit) => {
            // Scalar roots retain traversal order, not the absent operand's
            // slot. LIMIT n and OFFSET n have the same one-root sequence but
            // different relational semantics.
            fingerprint.write_u64(limit.limit.is_some() as u64);
            fingerprint.write_u64(limit.offset.is_some() as u64);
            encode_hnsw_options(&mut fingerprint, limit.hnsw_options);
        }
        LogicalOperator::Order(order) => {
            encode_orders(&mut fingerprint, &order.orders);
        }
        LogicalOperator::TopN(topn) => {
            fingerprint.write_u64(topn.limit as u64);
            fingerprint.write_u64(topn.offset as u64);
            encode_orders(&mut fingerprint, &topn.orders);
            encode_hnsw_options(&mut fingerprint, topn.hnsw_options);
        }
        LogicalOperator::ExpressionGet(values) => {
            fingerprint.write_u64(values.expressions.len() as u64);
            for row in &values.expressions {
                fingerprint.write_u64(row.len() as u64);
            }
        }
        LogicalOperator::Join(join) => match join {
            Join::Comparison(join) => {
                fingerprint.write_u64(0);
                fingerprint.write_u64(join.join_type as u64);
                fingerprint.write_u64(join.anti_join_mode as u64);
                fingerprint.write_u64(join.delim_flipped as u64);
                fingerprint.write_u64(match join.build_side_constraint {
                    paro_planner::operator::JoinBuildSideConstraint::Either => 0,
                    paro_planner::operator::JoinBuildSideConstraint::Left => 1,
                    paro_planner::operator::JoinBuildSideConstraint::Right => 2,
                });
                encode_optional_usize(&mut fingerprint, join.mark_index);
                match join.mark_semantics {
                    paro_planner::operator::MarkJoinSemantics::NotMark => fingerprint.write_u64(0),
                    paro_planner::operator::MarkJoinSemantics::TwoValued => {
                        fingerprint.write_u64(1)
                    }
                    paro_planner::operator::MarkJoinSemantics::ThreeValuedFrom(index) => {
                        fingerprint.write_u64(2);
                        fingerprint.write_u64(index as u64);
                    }
                }
            }
            Join::Any(join) => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(join.join_type as u64);
                fingerprint.write_u64(match join.build_side_constraint {
                    paro_planner::operator::JoinBuildSideConstraint::Either => 0,
                    paro_planner::operator::JoinBuildSideConstraint::Left => 1,
                    paro_planner::operator::JoinBuildSideConstraint::Right => 2,
                });
                encode_optional_usize(&mut fingerprint, join.mark_index);
            }
            Join::Cross(join) => {
                fingerprint.write_u64(2);
                fingerprint.write_u64(match join.build_side_constraint {
                    paro_planner::operator::JoinBuildSideConstraint::Either => 0,
                    paro_planner::operator::JoinBuildSideConstraint::Left => 1,
                    paro_planner::operator::JoinBuildSideConstraint::Right => 2,
                });
            }
        },
        LogicalOperator::DelimGet(_) => {}
        LogicalOperator::DependentJoin(join) => encode_dependent_join(&mut fingerprint, join),
        LogicalOperator::SetOperation(set) => {
            fingerprint.write_u64(set.setop_type as u64);
            fingerprint.write_u64(set.setop_all as u64);
            fingerprint.write_u64(set.allow_out_of_order as u64);
        }
        LogicalOperator::Distinct(distinct) => {
            fingerprint.write_u64(distinct.distinct_type as u64);
            match &distinct.order_by {
                None => fingerprint.write_u64(u64::MAX),
                Some(orders) => encode_orders(&mut fingerprint, orders),
            }
        }
        LogicalOperator::Window(window) => {
            fingerprint.write_u64(window.expressions.len() as u64);
        }
        LogicalOperator::EmptyResult(_) => {}
        LogicalOperator::Aggregate(aggregate) => {
            fingerprint.write_u64(aggregate.groups.len() as u64);
            fingerprint.write_u64(aggregate.aggregates.len() as u64);
            fingerprint.write_u64(aggregate.grouping_sets.len() as u64);
            for grouping_set in &aggregate.grouping_sets {
                encode_usizes(&mut fingerprint, &grouping_set.expressions);
            }
            fingerprint.write_u64(aggregate.grouping_functions.len() as u64);
            for grouping in &aggregate.grouping_functions {
                encode_usizes(&mut fingerprint, grouping);
            }
            fingerprint.write_u64(aggregate.group_dependencies.len() as u64);
            for dependency in &aggregate.group_dependencies {
                encode_usizes(&mut fingerprint, &dependency.determinants);
                encode_usizes(&mut fingerprint, &dependency.dependents);
            }
            fingerprint.write_u64(match aggregate.group_input_multiplicity {
                paro_planner::operator::GroupInputMultiplicity::Arbitrary => 0,
                paro_planner::operator::GroupInputMultiplicity::AtMostOne(_) => 1,
            });
            fingerprint.write_u64(aggregate.post_reduction.is_some() as u64);
        }
        LogicalOperator::MaterializedCTE(cte) => {
            fingerprint.write_u64(cte.cte_index as u64);
            fingerprint.write_u64(cte.output_columns.len() as u64);
            for column in &cte.output_columns {
                fingerprint.write_u64(column.definition.0 as u64);
            }
            fingerprint.write_u64(match cte.materialized {
                paro_planner::binder::ir::CTEMaterialize::Default => 0,
                paro_planner::binder::ir::CTEMaterialize::Materialized => 1,
                paro_planner::binder::ir::CTEMaterialize::NotMaterialized => 2,
            });
            fingerprint.write_u64(cte.ref_count as u64);
        }
        LogicalOperator::RecursiveCTE(cte) => {
            fingerprint.write_u64(cte.cte_index as u64);
            fingerprint.write_u64(cte.union_all as u64);
        }
        LogicalOperator::CTERef(cte) => {
            fingerprint.write_u64(cte.cte_index as u64);
            fingerprint.write_u64(cte.definition_columns.len() as u64);
            for column in &cte.definition_columns {
                fingerprint.write_u64(column.0 as u64);
            }
        }
        LogicalOperator::TableFunctionGet(function) => {
            encode_table_function(&mut fingerprint, function);
        }
        LogicalOperator::SearchScan(search) => {
            encode_get(&mut fingerprint, &search.get);
            encode_search_request(&mut fingerprint, &search.request);
            fingerprint.write_u64(
                search
                    .score_output_index
                    .and_then(|index| u64::try_from(index).ok())
                    .unwrap_or(u64::MAX),
            );
            fingerprint.write_u64(search.order_ascending as u64);
            fingerprint.write_u64(search.limit as u64);
        }
        LogicalOperator::FullTextFilterScan(search) => {
            encode_get(&mut fingerprint, &search.get);
            encode_search_request(&mut fingerprint, &search.request);
        }
        LogicalOperator::GraphMatch(graph) => {
            fingerprint.write_u64(graph.graph_entry.object_id().raw());
            fingerprint.write_u64(graph.table_index as u64);
            fingerprint.write_bytes(graph.relation_alias.as_bytes());
            encode_graph_pattern(&mut fingerprint, &graph.bound_pattern);
            fingerprint.write_u64(graph.columns.len() as u64);
            for column in &graph.columns {
                fingerprint.write_bytes(column.alias.as_bytes());
                paro_planner::physical::scalar_identity::encode_logical_type(
                    &mut fingerprint,
                    &column.logical_type,
                );
            }
            fingerprint.write_u64(match graph.path_mode.as_ref() {
                None => 0,
                Some(paro_parser::ast::PathMode::AnyShortest) => 1,
                Some(paro_parser::ast::PathMode::AllShortest) => 2,
                Some(paro_parser::ast::PathMode::Any) => 3,
                Some(paro_parser::ast::PathMode::All) => 4,
            });
            fingerprint.write_u64(graph.has_path_functions as u64);
        }
        LogicalOperator::GraphScan(scan) => {
            // A graph variable's table index is part of the carrier ABI. Two
            // scans of the same vertex relation are not interchangeable when
            // downstream expands address their local-id slots by variable.
            fingerprint.write_u64(scan.table_index as u64);
            fingerprint.write_u64(scan.output_table_index as u64);
            fingerprint.write_bytes(scan.schema_name.as_bytes());
            fingerprint.write_bytes(scan.graph_name.as_bytes());
            fingerprint.write_bytes(scan.label.as_bytes());
            fingerprint.write_u64(scan.vertex_info.table_oid);
            fingerprint.write_bytes(scan.vertex_info.table_name.as_bytes());
            encode_u32s(&mut fingerprint, &scan.vertex_info.key_column_ids);
            encode_u32s(&mut fingerprint, &scan.vertex_info.property_column_ids);
        }
        LogicalOperator::GraphExpand(expand) => {
            fingerprint.write_u64(expand.source_table_index as u64);
            fingerprint.write_u64(expand.edge_table_index as u64);
            fingerprint.write_u64(expand.target_table_index as u64);
            fingerprint.write_u64(expand.output_table_index as u64);
            fingerprint.write_u64(expand.edge_info.table_oid);
            fingerprint.write_bytes(expand.edge_info.table_name.as_bytes());
            fingerprint.write_bytes(expand.edge_info.label.as_bytes());
            fingerprint.write_bytes(expand.source_label.as_bytes());
            fingerprint.write_bytes(expand.target_label.as_bytes());
            fingerprint.write_u64(expand.source_table_oid);
            fingerprint.write_u64(expand.target_table_oid);
            fingerprint.write_bytes(expand.target_table_name.as_bytes());
            fingerprint.write_u64(match expand.direction {
                paro_planner::operator::ExpandDirection::Forward => 0,
                paro_planner::operator::ExpandDirection::Backward => 1,
                paro_planner::operator::ExpandDirection::Both => 2,
            });
            fingerprint.write_bytes(
                expand
                    .quantifier
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
                    .as_bytes(),
            );
            fingerprint.write_bytes(
                expand
                    .path_mode
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
                    .as_bytes(),
            );
            fingerprint.write_u64(expand.has_path_functions as u64);
            encode_u32s(&mut fingerprint, &expand.edge_info.key_column_ids);
            encode_u32s(&mut fingerprint, &expand.edge_info.source_key_column_ids);
            fingerprint.write_bytes(expand.edge_info.source_vertex_table.as_bytes());
            encode_u32s(&mut fingerprint, &expand.edge_info.source_ref_column_ids);
            encode_u32s(
                &mut fingerprint,
                &expand.edge_info.destination_key_column_ids,
            );
            fingerprint.write_bytes(expand.edge_info.destination_vertex_table.as_bytes());
            encode_u32s(
                &mut fingerprint,
                &expand.edge_info.destination_ref_column_ids,
            );
            encode_u32s(&mut fingerprint, &expand.edge_info.property_column_ids);
        }
        LogicalOperator::DummyScan => {}
        LogicalOperator::Insert(_)
        | LogicalOperator::Delete(_)
        | LogicalOperator::Update(_)
        | LogicalOperator::CopyTo(_)
        | LogicalOperator::Explain(_)
        | LogicalOperator::Alter(_)
        | LogicalOperator::CreateTable(_)
        | LogicalOperator::CreateRoutine(_)
        | LogicalOperator::CreateSequence(_)
        | LogicalOperator::CreateSchema(_)
        | LogicalOperator::CreateIndex(_)
        | LogicalOperator::CreateView(_)
        | LogicalOperator::Drop(_)
        | LogicalOperator::CreatePropertyGraph(_)
        | LogicalOperator::DropPropertyGraph(_)
        | LogicalOperator::RefreshPropertyGraph(_) => {
            return Err(paro_error::internal(
                "statement operator crossed the statement/query boundary into Memo",
            ));
        }
    }
    Ok(fingerprint.finish_recording())
}

pub(super) fn encode_graph_pattern(
    fingerprint: &mut StableFingerprintBuilder,
    pattern: &paro_planner::binder::ir::BoundGraphPattern,
) {
    use paro_planner::binder::bind::graph::BoundPatternElement;

    fingerprint.write_u64(pattern.elements.len() as u64);
    for element in &pattern.elements {
        match element {
            BoundPatternElement::Vertex(vertex) => {
                fingerprint.write_u64(0);
                fingerprint.write_bytes(vertex.variable_name.as_bytes());
                fingerprint.write_u64(vertex.table_index as u64);
                encode_vertex_table(fingerprint, &vertex.vertex_table_info);
                encode_column_bindings(fingerprint, &vertex.column_bindings);
                encode_strings(fingerprint, &vertex.column_names);
                fingerprint.write_u64(vertex.filter.is_some() as u64);
            }
            BoundPatternElement::Edge(edge) => {
                fingerprint.write_u64(1);
                fingerprint.write_bytes(edge.variable_name.as_bytes());
                fingerprint.write_u64(edge.table_index as u64);
                encode_edge_table(fingerprint, &edge.edge_table_info);
                encode_column_bindings(fingerprint, &edge.column_bindings);
                encode_strings(fingerprint, &edge.column_names);
                fingerprint.write_u64(match edge.direction {
                    paro_parser::ast::EdgeDirection::Right => 0,
                    paro_parser::ast::EdgeDirection::Left => 1,
                    paro_parser::ast::EdgeDirection::Undirected => 2,
                    paro_parser::ast::EdgeDirection::LeftRight => 3,
                });
                match &edge.quantifier {
                    None => fingerprint.write_u64(0),
                    Some(paro_parser::ast::PathQuantifier::Plus) => fingerprint.write_u64(1),
                    Some(paro_parser::ast::PathQuantifier::Star) => fingerprint.write_u64(2),
                    Some(paro_parser::ast::PathQuantifier::Bounded { lower, upper }) => {
                        fingerprint.write_u64(3);
                        fingerprint.write_u64(*lower);
                        match upper {
                            None => fingerprint.write_u64(0),
                            Some(upper) => {
                                fingerprint.write_u64(1);
                                fingerprint.write_u64(*upper);
                            }
                        }
                    }
                }
                fingerprint.write_u64(edge.filter.is_some() as u64);
                fingerprint.write_bytes(edge.source_variable.as_bytes());
                fingerprint.write_bytes(edge.destination_variable.as_bytes());
            }
        }
    }
}

pub(super) fn encode_vertex_table(
    fingerprint: &mut StableFingerprintBuilder,
    table: &paro_catalog::entry::VertexTableInfo,
) {
    fingerprint.write_bytes(table.table_name.as_bytes());
    fingerprint.write_u64(table.table_oid);
    encode_u32s(fingerprint, &table.key_column_ids);
    fingerprint.write_bytes(table.label.as_bytes());
    encode_u32s(fingerprint, &table.property_column_ids);
}

pub(super) fn encode_edge_table(
    fingerprint: &mut StableFingerprintBuilder,
    table: &paro_catalog::entry::EdgeTableInfo,
) {
    fingerprint.write_bytes(table.table_name.as_bytes());
    fingerprint.write_u64(table.table_oid);
    encode_u32s(fingerprint, &table.key_column_ids);
    encode_u32s(fingerprint, &table.source_key_column_ids);
    fingerprint.write_bytes(table.source_vertex_table.as_bytes());
    encode_u32s(fingerprint, &table.source_ref_column_ids);
    encode_u32s(fingerprint, &table.destination_key_column_ids);
    fingerprint.write_bytes(table.destination_vertex_table.as_bytes());
    encode_u32s(fingerprint, &table.destination_ref_column_ids);
    fingerprint.write_bytes(table.label.as_bytes());
    encode_u32s(fingerprint, &table.property_column_ids);
}

pub(super) fn encode_column_bindings(
    fingerprint: &mut StableFingerprintBuilder,
    bindings: &[ColumnBinding],
) {
    fingerprint.write_u64(bindings.len() as u64);
    for binding in bindings {
        fingerprint.write_u64(binding.table_index as u64);
        fingerprint.write_u64(binding.column_index as u64);
    }
}

pub(super) fn encode_strings(fingerprint: &mut StableFingerprintBuilder, values: &[String]) {
    fingerprint.write_u64(values.len() as u64);
    for value in values {
        fingerprint.write_bytes(value.as_bytes());
    }
}

pub(super) fn encode_get(
    fingerprint: &mut StableFingerprintBuilder,
    get: &paro_planner::operator::Get,
) {
    // The same catalog object may occur more than once in a query. The bound
    // table index is part of the carrier ABI and distinguishes those aliases.
    fingerprint.write_u64(get.table_index as u64);
    fingerprint.write_u64(
        get.table
            .as_ref()
            .map(|table| table.object_id().raw())
            .unwrap_or(0),
    );
    fingerprint.write_u64(get.column_sources.len() as u64);
    for source in &get.column_sources {
        match source {
            paro_planner::operator::GetColumnSource::Stored { column_id } => {
                fingerprint.write_u64(0);
                fingerprint.write_u64(*column_id as u64);
            }
            paro_planner::operator::GetColumnSource::MatchedUtf8Prefix {
                source_column,
                byte_width,
            } => {
                fingerprint.write_u64(1);
                fingerprint.write_u64(*source_column as u64);
                fingerprint.write_u64(*byte_width as u64);
            }
            paro_planner::operator::GetColumnSource::VirtualRowId => {
                fingerprint.write_u64(2);
            }
        }
    }
    fingerprint.write_u64(get.column_types.len() as u64);
    for ty in &get.column_types {
        paro_planner::physical::scalar_identity::encode_logical_type(fingerprint, ty);
    }
    match &get.scan_order {
        None => fingerprint.write_u64(u64::MAX),
        Some(order) => {
            fingerprint.write_u64(order.column_idx as u64);
            fingerprint.write_u64(order.order_by as u64);
            fingerprint.write_u64(order.order_type as u64);
            fingerprint.write_u64(order.column_type as u64);
            encode_optional_usize(fingerprint, order.row_limit);
            fingerprint.write_u64(order.row_offset as u64);
        }
    }
}

pub(super) fn encode_external_call(
    fingerprint: &mut StableFingerprintBuilder,
    call: &paro_external::routine::bound::BoundRoutineCallMeta,
) {
    use paro_external::routine::boundary::PlacementClass;
    use paro_external::routine::spec::{
        RoutineNullPolicy, RoutineSideEffects, RoutineStability, RowSemantics,
    };

    encode_routine_identity(fingerprint, &call.identity);
    fingerprint.write_u64(match call.boundary.placement {
        PlacementClass::Native => 0,
        PlacementClass::External => 1,
    });
    fingerprint.write_u64(call.boundary.may_block as u64);
    fingerprint.write_u64(match call.boundary.row_semantics {
        RowSemantics::RowPreserving => 0,
        RowSemantics::RelationExpanding => 1,
        RowSemantics::Aggregate => 2,
        RowSemantics::Window => 3,
    });
    fingerprint.write_u64(match call.semantics.stability {
        RoutineStability::Immutable => 0,
        RoutineStability::Stable => 1,
        RoutineStability::Volatile => 2,
    });
    fingerprint.write_u64(match call.semantics.null_policy {
        RoutineNullPolicy::Strict => 0,
        RoutineNullPolicy::CalledOnNullInput => 1,
    });
    fingerprint.write_u64(match call.semantics.side_effects {
        RoutineSideEffects::None => 0,
        RoutineSideEffects::HasSideEffects => 1,
    });
    fingerprint.write_u64(match call.semantics.row_semantics {
        RowSemantics::RowPreserving => 0,
        RowSemantics::RelationExpanding => 1,
        RowSemantics::Aggregate => 2,
        RowSemantics::Window => 3,
    });
    fingerprint.write_u64(call.semantics.may_block as u64);
}

pub(super) fn encode_dependent_join<Child>(
    fingerprint: &mut StableFingerprintBuilder,
    join: &paro_planner::operator::DependentJoin<Child>,
) {
    use paro_planner::operator::{DependentJoinKind, MarkSubqueryKind};

    fingerprint.write_u64(join.correlated_columns.len() as u64);
    for correlation in &join.correlated_columns {
        fingerprint.write_u64(correlation.table_index as u64);
        fingerprint.write_u64(correlation.column_index as u64);
        paro_planner::physical::scalar_identity::encode_logical_type(
            fingerprint,
            &correlation.return_type,
        );
        fingerprint.write_u64(correlation.depth as u64);
    }
    match &join.kind {
        DependentJoinKind::Scalar { presence_binding } => {
            fingerprint.write_u64(0);
            if let Some(binding) = presence_binding {
                fingerprint.write_u64(1);
                fingerprint.write_u64(binding.table_index as u64);
                fingerprint.write_u64(binding.column_index as u64);
            } else {
                fingerprint.write_u64(0);
            }
        }
        DependentJoinKind::Mark {
            mark_index,
            subquery,
        } => {
            fingerprint.write_u64(1);
            fingerprint.write_u64(*mark_index as u64);
            match subquery {
                MarkSubqueryKind::Exists => fingerprint.write_u64(0),
                MarkSubqueryKind::NotExists => fingerprint.write_u64(1),
                MarkSubqueryKind::Any(payload) | MarkSubqueryKind::All(payload) => {
                    fingerprint.write_u64(if matches!(subquery, MarkSubqueryKind::Any(_)) {
                        2
                    } else {
                        3
                    });
                    fingerprint.write_u64(payload.comparison_type as u64);
                    fingerprint.write_u64(payload.child_types.len() as u64);
                    for ty in &payload.child_types {
                        paro_planner::physical::scalar_identity::encode_logical_type(
                            fingerprint,
                            ty,
                        );
                    }
                    fingerprint.write_u64(payload.child_targets.len() as u64);
                    for ty in &payload.child_targets {
                        paro_planner::physical::scalar_identity::encode_logical_type(
                            fingerprint,
                            ty,
                        );
                    }
                }
            }
        }
        DependentJoinKind::Lateral {
            join_type,
            join_condition,
        } => {
            fingerprint.write_u64(2);
            fingerprint.write_u64(*join_type as u64);
            fingerprint.write_u64(join_condition.is_some() as u64);
        }
    }
}

pub(super) fn encode_table_function(
    fingerprint: &mut StableFingerprintBuilder,
    function: &paro_planner::operator::TableFunctionGet,
) {
    fingerprint.write_bytes(function.function.name.as_bytes());
    fingerprint.write_u64(function.function.arguments.len() as u64);
    for ty in &function.function.arguments {
        paro_planner::physical::scalar_identity::encode_logical_type(fingerprint, ty);
    }
    fingerprint.write_u64(function.function.projection_pushdown as u64);
    fingerprint.write_u64(function.function.filter_pushdown as u64);
    match &function.function.varargs {
        None => fingerprint.write_u64(0),
        Some(ty) => {
            fingerprint.write_u64(1);
            paro_planner::physical::scalar_identity::encode_logical_type(fingerprint, ty);
        }
    }
    fingerprint.write_u64(function.function.named_parameters.len() as u64);
    for (name, ty) in &function.function.named_parameters {
        fingerprint.write_bytes(name.as_bytes());
        paro_planner::physical::scalar_identity::encode_logical_type(fingerprint, ty);
    }
    match &function.projection_ids {
        None => fingerprint.write_u64(u64::MAX),
        Some(projection) => encode_usizes(fingerprint, projection),
    }
    fingerprint.write_u64(function.input_table_types.len() as u64);
    for ty in &function.input_table_types {
        paro_planner::physical::scalar_identity::encode_logical_type(fingerprint, ty);
    }
    fingerprint.write_u64(function.with_ordinality as u64);
    match &function.bind_data {
        None => fingerprint.write_u64(0),
        Some(bind_data) => {
            fingerprint.write_u64(1);
            encode_optional_usize(fingerprint, bind_data.as_ref().cardinality());
        }
    }
}

pub(super) fn encode_projection_map(
    fingerprint: &mut StableFingerprintBuilder,
    projection: &paro_planner::operator::ProjectionMap,
) {
    match projection.as_columns() {
        None => fingerprint.write_u64(u64::MAX),
        Some(columns) => {
            fingerprint.write_u64(columns.len() as u64);
            for column in columns {
                fingerprint.write_u64(*column as u64);
            }
        }
    }
}

pub(super) fn encode_orders(fingerprint: &mut StableFingerprintBuilder, orders: &[OrderByNode]) {
    fingerprint.write_u64(orders.len() as u64);
    for order in orders {
        fingerprint.write_u64(order.ascending as u64);
        fingerprint.write_u64(order.nulls_first as u64);
    }
}

pub(super) fn operator_tag(operator: LogicalOperatorType) -> u64 {
    match operator {
        LogicalOperatorType::Get => 0,
        LogicalOperatorType::Filter => 1,
        LogicalOperatorType::Projection => 2,
        LogicalOperatorType::RowFetch => 3,
        LogicalOperatorType::ExternalProject => 4,
        LogicalOperatorType::ExternalTable => 5,
        LogicalOperatorType::Limit => 6,
        LogicalOperatorType::Order => 7,
        LogicalOperatorType::TopN => 8,
        LogicalOperatorType::Alter => 9,
        LogicalOperatorType::CreateTable => 10,
        LogicalOperatorType::CreateRoutine => 11,
        LogicalOperatorType::CreateSequence => 12,
        LogicalOperatorType::CreateSchema => 13,
        LogicalOperatorType::CreateIndex => 14,
        LogicalOperatorType::Drop => 15,
        LogicalOperatorType::Insert => 16,
        LogicalOperatorType::Delete => 17,
        LogicalOperatorType::Update => 18,
        LogicalOperatorType::LogicalCopy => 19,
        LogicalOperatorType::Explain => 20,
        LogicalOperatorType::EmptyResult => 21,
        LogicalOperatorType::Aggregate => 22,
        LogicalOperatorType::ComparisonJoin => 23,
        LogicalOperatorType::AnyJoin => 24,
        LogicalOperatorType::CrossProduct => 25,
        LogicalOperatorType::DelimGet => 26,
        LogicalOperatorType::DependentJoin => 27,
        LogicalOperatorType::LogicalUnion => 28,
        LogicalOperatorType::LogicalIntersect => 29,
        LogicalOperatorType::LogicalExcept => 30,
        LogicalOperatorType::Distinct => 31,
        LogicalOperatorType::Window => 32,
        LogicalOperatorType::MaterializedCTE => 33,
        LogicalOperatorType::RecursiveCTE => 34,
        LogicalOperatorType::CTERef => 35,
        LogicalOperatorType::TableFunctionGet => 36,
        LogicalOperatorType::SearchScan => 37,
        LogicalOperatorType::FullTextFilterScan => 38,
        LogicalOperatorType::CreateView => 39,
        LogicalOperatorType::CreatePropertyGraph => 40,
        LogicalOperatorType::DropPropertyGraph => 41,
        LogicalOperatorType::RefreshPropertyGraph => 42,
        LogicalOperatorType::GraphMatch => 43,
        LogicalOperatorType::GraphScan => 44,
        LogicalOperatorType::GraphExpand => 45,
        LogicalOperatorType::BoundReference => 46,
    }
}
