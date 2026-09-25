// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! Statement/query boundary for the optimizer.
//!
//! The planner delivers a bound tree. This module consumes it once and makes
//! the architectural split explicit: only the query child can enter Memo;
//! statement side effects are reattached after winner extraction.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::physical::identity::{BaseRelationId, SnapshotId};
use crate::physical::requirements::MutationSafetyRequirement;
use paro_catalog::entry::{CatalogEntry, ConstraintType, TableCatalogEntry};
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_function::copy::{CopyFunctionBindData, CopyOptions, CopyToFunction};
use paro_parser::ast::CopySource;
use paro_planner::expression::Expression;
use paro_planner::operator::{
    CopyTo, Delete, Explain, ExplainSpec, Insert, InsertOnConflict, LogicalOperator, Update,
};
use paro_planner::physical::{ReturningImageContract, WriteContract};
use paro_planner::plan::{CardinalityEstimate, NodeStats, OwnedLogicalPlan, PlanNodeId};

#[derive(Debug, Clone)]
pub(crate) struct ExplainEnvelope {
    pub id: PlanNodeId,
    pub stats: NodeStats,
    pub spec: ExplainSpec,
    pub logical_plan_unopt: Option<String>,
    pub logical_plan_opt: Option<String>,
}

impl ExplainEnvelope {
    pub(crate) fn attach(&self, child: OwnedLogicalPlan) -> OwnedLogicalPlan {
        OwnedLogicalPlan {
            id: self.id,
            stats: self.stats.clone(),
            operator: LogicalOperator::Explain(Explain {
                child: Box::new(child),
                spec: self.spec,
                logical_plan_unopt: self.logical_plan_unopt.clone(),
                logical_plan_opt: self.logical_plan_opt.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum QueryStatementLayer {
    Query,
    Insert {
        id: PlanNodeId,
        stats: NodeStats,
        table: Arc<TableCatalogEntry>,
        column_index_map: Vec<usize>,
        expected_types: Vec<LogicalType>,
        on_conflict: Option<InsertOnConflict>,
        write: WriteContract,
    },
    Delete {
        id: PlanNodeId,
        stats: NodeStats,
        table: Arc<TableCatalogEntry>,
        table_index: u32,
        return_chunk: bool,
        is_full_table_delete: bool,
        write: WriteContract,
    },
    Update {
        id: PlanNodeId,
        stats: NodeStats,
        table: Arc<TableCatalogEntry>,
        table_index: u32,
        return_chunk: bool,
        columns: Vec<usize>,
        expressions: Vec<Expression>,
        write: WriteContract,
    },
    CopyTo {
        id: PlanNodeId,
        stats: NodeStats,
        copy_function: CopyToFunction,
        bind_data: Arc<dyn CopyFunctionBindData>,
        file_path: String,
        source: CopySource,
        options: CopyOptions,
        names: Vec<String>,
        types: Vec<LogicalType>,
    },
}

impl QueryStatementLayer {
    pub(crate) fn result_cardinality(&self) -> Option<CardinalityEstimate> {
        match self {
            Self::Query => None,
            Self::CopyTo { .. } | Self::Insert { .. } => Some(CardinalityEstimate::exact(1)),
            Self::Delete { return_chunk, .. } | Self::Update { return_chunk, .. } => {
                (!return_chunk).then(|| CardinalityEstimate::exact(1))
            }
        }
    }

    pub(crate) fn stable_tag(&self) -> u64 {
        match self {
            Self::Query => 0,
            Self::Insert { .. } => 1,
            Self::Delete { .. } => 2,
            Self::Update { .. } => 3,
            Self::CopyTo { .. } => 4,
        }
    }

    pub(crate) fn write_contract(&self) -> Option<&WriteContract> {
        match self {
            Self::Insert { write, .. }
            | Self::Delete { write, .. }
            | Self::Update { write, .. } => Some(write),
            Self::Query | Self::CopyTo { .. } => None,
        }
    }

    pub(crate) fn attach(&self, query: OwnedLogicalPlan) -> OwnedLogicalPlan {
        match self {
            Self::Query => query,
            Self::Insert {
                id,
                stats,
                table,
                column_index_map,
                expected_types,
                on_conflict,
                ..
            } => OwnedLogicalPlan {
                id: *id,
                stats: stats.clone(),
                operator: LogicalOperator::Insert(Insert {
                    table: table.clone(),
                    column_index_map: column_index_map.clone(),
                    expected_types: expected_types.clone(),
                    on_conflict: on_conflict.clone(),
                    child: Box::new(query),
                }),
            },
            Self::Delete {
                id,
                stats,
                table,
                table_index,
                return_chunk,
                is_full_table_delete,
                ..
            } => OwnedLogicalPlan {
                id: *id,
                stats: stats.clone(),
                operator: LogicalOperator::Delete(Delete {
                    table: table.clone(),
                    table_index: *table_index,
                    return_chunk: *return_chunk,
                    is_full_table_delete: *is_full_table_delete,
                    child: Box::new(query),
                }),
            },
            Self::Update {
                id,
                stats,
                table,
                table_index,
                return_chunk,
                columns,
                expressions,
                ..
            } => OwnedLogicalPlan {
                id: *id,
                stats: stats.clone(),
                operator: LogicalOperator::Update(Update {
                    table: table.clone(),
                    table_index: *table_index,
                    return_chunk: *return_chunk,
                    columns: columns.clone(),
                    expressions: expressions.clone(),
                    child: Box::new(query),
                }),
            },
            Self::CopyTo {
                id,
                stats,
                copy_function,
                bind_data,
                file_path,
                source,
                options,
                names,
                types,
            } => OwnedLogicalPlan {
                id: *id,
                stats: stats.clone(),
                operator: LogicalOperator::CopyTo(Box::new(CopyTo {
                    copy_function: copy_function.clone(),
                    bind_data: bind_data.clone(),
                    file_path: file_path.clone(),
                    source: source.clone(),
                    options: options.clone(),
                    child: Box::new(query),
                    names: names.clone(),
                    types: types.clone(),
                })),
            },
        }
    }
}

#[derive(Debug)]
pub(crate) enum StatementBody {
    Query {
        query: Box<OwnedLogicalPlan>,
        layer: Box<QueryStatementLayer>,
    },
    Utility(Box<OwnedLogicalPlan>),
}

#[derive(Debug)]
pub(crate) struct StatementPlan {
    pub explain: Option<ExplainEnvelope>,
    pub body: StatementBody,
}

impl StatementPlan {
    pub(crate) fn split(plan: OwnedLogicalPlan, snapshot_version: u64) -> Result<Self> {
        let (explain, plan) = detach_explain(plan)?;
        let (id, stats, operator) = plan.into_parts();
        let body = match operator {
            LogicalOperator::Insert(insert) => {
                let reads_target =
                    reads_target(insert.child.as_ref(), insert.table.object_id().raw());
                let write = write_contract(
                    insert.table.as_ref(),
                    insert.column_index_map.iter().copied(),
                    snapshot_version,
                    reads_target,
                    ReturningImageContract::CountOnly,
                );
                StatementBody::Query {
                    query: insert.child,
                    layer: Box::new(QueryStatementLayer::Insert {
                        id,
                        stats,
                        table: insert.table,
                        column_index_map: insert.column_index_map,
                        expected_types: insert.expected_types,
                        on_conflict: insert.on_conflict,
                        write,
                    }),
                }
            }
            LogicalOperator::Delete(delete) => {
                let returning = if delete.return_chunk {
                    ReturningImageContract::BeforeImage
                } else {
                    ReturningImageContract::CountOnly
                };
                let write = write_contract(
                    delete.table.as_ref(),
                    std::iter::empty(),
                    snapshot_version,
                    reads_target(delete.child.as_ref(), delete.table.object_id().raw()),
                    returning,
                );
                StatementBody::Query {
                    query: delete.child,
                    layer: Box::new(QueryStatementLayer::Delete {
                        id,
                        stats,
                        table: delete.table,
                        table_index: delete.table_index,
                        return_chunk: delete.return_chunk,
                        is_full_table_delete: delete.is_full_table_delete,
                        write,
                    }),
                }
            }
            LogicalOperator::Update(update) => {
                let returning = if update.return_chunk {
                    ReturningImageContract::AfterImage
                } else {
                    ReturningImageContract::CountOnly
                };
                let write = write_contract(
                    update.table.as_ref(),
                    update.columns.iter().copied(),
                    snapshot_version,
                    reads_target(update.child.as_ref(), update.table.object_id().raw()),
                    returning,
                );
                StatementBody::Query {
                    query: update.child,
                    layer: Box::new(QueryStatementLayer::Update {
                        id,
                        stats,
                        table: update.table,
                        table_index: update.table_index,
                        return_chunk: update.return_chunk,
                        columns: update.columns,
                        expressions: update.expressions,
                        write,
                    }),
                }
            }
            LogicalOperator::CopyTo(copy) => StatementBody::Query {
                query: copy.child,
                layer: Box::new(QueryStatementLayer::CopyTo {
                    id,
                    stats,
                    copy_function: copy.copy_function,
                    bind_data: copy.bind_data,
                    file_path: copy.file_path,
                    source: copy.source,
                    options: copy.options,
                    names: copy.names,
                    types: copy.types,
                }),
            },
            utility @ (LogicalOperator::CreateTable(_)
            | LogicalOperator::CreateRoutine(_)
            | LogicalOperator::Alter(_)
            | LogicalOperator::CreateSequence(_)
            | LogicalOperator::CreateSchema(_)
            | LogicalOperator::CreateIndex(_)
            | LogicalOperator::CreateView(_)
            | LogicalOperator::Drop(_)
            | LogicalOperator::CreatePropertyGraph(_)
            | LogicalOperator::DropPropertyGraph(_)
            | LogicalOperator::RefreshPropertyGraph(_)) => {
                StatementBody::Utility(Box::new(OwnedLogicalPlan {
                    id,
                    stats,
                    operator: utility,
                }))
            }
            query => StatementBody::Query {
                query: Box::new(OwnedLogicalPlan {
                    id,
                    stats,
                    operator: query,
                }),
                layer: Box::new(QueryStatementLayer::Query),
            },
        };
        if explain.is_some() && matches!(body, StatementBody::Utility(_)) {
            return Err(paro_error::not_supported(
                "EXPLAIN of utility/DDL statements is not a query optimization target",
            ));
        }
        Ok(Self { explain, body })
    }
}

fn detach_explain(plan: OwnedLogicalPlan) -> Result<(Option<ExplainEnvelope>, OwnedLogicalPlan)> {
    let (id, stats, operator) = plan.into_parts();
    match operator {
        LogicalOperator::Explain(Explain {
            child,
            spec,
            logical_plan_unopt,
            logical_plan_opt,
        }) => {
            if matches!(child.operator, LogicalOperator::Explain(_)) {
                return Err(paro_error::invalid_input("nested EXPLAIN is not supported"));
            }
            Ok((
                Some(ExplainEnvelope {
                    id,
                    stats,
                    spec,
                    logical_plan_unopt,
                    logical_plan_opt,
                }),
                *child,
            ))
        }
        operator => Ok((
            None,
            OwnedLogicalPlan {
                id,
                stats,
                operator,
            },
        )),
    }
}

fn write_contract(
    table: &TableCatalogEntry,
    modified_columns: impl IntoIterator<Item = usize>,
    snapshot_version: u64,
    reads_target: bool,
    returning: ReturningImageContract,
) -> WriteContract {
    let modified_columns = modified_columns.into_iter().collect::<BTreeSet<_>>();
    let key_columns = table
        .constraints()
        .iter()
        .filter(|constraint| {
            matches!(
                constraint.constraint_type,
                ConstraintType::PrimaryKey | ConstraintType::Unique
            )
        })
        .flat_map(|constraint| constraint.columns.iter().copied())
        .collect::<BTreeSet<_>>();
    let modified_key_columns = modified_columns
        .intersection(&key_columns)
        .copied()
        .collect();
    let target_relation = BaseRelationId(0);
    let snapshot = SnapshotId(0);
    WriteContract {
        target_relation,
        target_object_id: table.object_id().raw(),
        modified_columns,
        modified_key_columns,
        snapshot_version,
        mutation_safety: if reads_target {
            MutationSafetyRequirement::StableReadBeforeWrite {
                targets: [target_relation].into_iter().collect(),
                snapshot,
            }
        } else {
            MutationSafetyRequirement::None
        },
        returning,
    }
}

fn reads_target(plan: &OwnedLogicalPlan, target_object_id: u64) -> bool {
    let reads_here = match &plan.operator {
        LogicalOperator::Get(get) => get
            .table
            .as_ref()
            .is_some_and(|table| table.object_id().raw() == target_object_id),
        LogicalOperator::SearchScan(scan) => scan
            .get
            .table
            .as_ref()
            .is_some_and(|table| table.object_id().raw() == target_object_id),
        LogicalOperator::FullTextFilterScan(scan) => scan
            .get
            .table
            .as_ref()
            .is_some_and(|table| table.object_id().raw() == target_object_id),
        _ => false,
    };
    reads_here
        || plan
            .children()
            .into_iter()
            .any(|child| reads_target(child, target_object_id))
}
