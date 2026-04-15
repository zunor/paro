// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::recovery::replay_handler::CatalogReplayHandler;
use paro_catalog::entry::CatalogObjectId;
use paro_catalog::entry::{Constraint, DependencyList};
use paro_common::ddl::{DdlDependencyRef, DdlStorageDescriptor};
use paro_common::error as paro_error;
use paro_storage::table::storage_descriptor::TableStorageDescriptor;
use paro_storage::wal::wal_entry::{TableConstraintInfo, WalConstraintType};

impl<'a> CatalogReplayHandler<'a> {
    pub(in crate::recovery) fn dependency_list_from_payload(
        payload: &[DdlDependencyRef],
    ) -> paro_common::error::Result<DependencyList> {
        let mut dependencies = DependencyList::new();
        for dependency in payload {
            let kind = match dependency.object.kind.as_str() {
                "TABLE" => paro_catalog::entry::CatalogType::Table,
                "SCHEMA" => paro_catalog::entry::CatalogType::Schema,
                "VIEW" => paro_catalog::entry::CatalogType::View,
                "INDEX" => paro_catalog::entry::CatalogType::Index,
                "PROPERTY_GRAPH" => paro_catalog::entry::CatalogType::PropertyGraph,
                "SEQUENCE" => paro_catalog::entry::CatalogType::Sequence,
                "SCALAR_FUNCTION" => paro_catalog::entry::CatalogType::ScalarFunction,
                "AGGREGATE_FUNCTION" => paro_catalog::entry::CatalogType::AggregateFunction,
                "TABLE_FUNCTION" => paro_catalog::entry::CatalogType::TableFunction,
                "COPY_FUNCTION" => paro_catalog::entry::CatalogType::CopyFunction,
                "TYPE" => paro_catalog::entry::CatalogType::Type,
                "COLLATION" => paro_catalog::entry::CatalogType::Collation,
                "DATABASE" => paro_catalog::entry::CatalogType::Database,
                other => {
                    return Err(paro_error::serialization_error(format!(
                        "unsupported dependency kind in WAL payload: {}",
                        other
                    )))
                }
            };
            let dependency_type = match dependency.dependency_type.as_str() {
                "regular" => paro_catalog::entry::DependencyType::Regular,
                "automatic" => paro_catalog::entry::DependencyType::Automatic,
                "owns" => paro_catalog::entry::DependencyType::Owns,
                "owned_by" => paro_catalog::entry::DependencyType::OwnedBy,
                other => {
                    return Err(paro_error::serialization_error(format!(
                        "unsupported dependency type in WAL payload: {}",
                        other
                    )))
                }
            };
            dependencies.add_dependency(
                paro_catalog::entry::CatalogObjectRef::new(
                    CatalogObjectId::from_raw(dependency.object.object_id),
                    kind,
                    dependency.object.catalog_name.clone(),
                    dependency.object.schema_id.map(CatalogObjectId::from_raw),
                    dependency.object.schema_name.clone(),
                    dependency.object.name.clone(),
                ),
                dependency_type,
            );
        }
        Ok(dependencies)
    }

    pub(in crate::recovery) fn decode_constraint_columns(
        columns: &[u32],
        column_count: usize,
        constraint_type: WalConstraintType,
    ) -> paro_common::error::Result<Vec<usize>> {
        let mut result = Vec::with_capacity(columns.len());
        for &column in columns {
            let column_idx = usize::try_from(column).map_err(|_| {
                paro_error::serialization_error(format!(
                    "WAL {:?} constraint column index {} overflows usize",
                    constraint_type, column
                ))
            })?;
            if column_idx >= column_count {
                return Err(paro_error::serialization_error(format!(
                    "WAL {:?} constraint column index {} out of bounds (column count {})",
                    constraint_type, column_idx, column_count
                )));
            }
            result.push(column_idx);
        }
        Ok(result)
    }

    pub(in crate::recovery) fn decode_constraints(
        constraints: &[TableConstraintInfo],
        column_count: usize,
    ) -> paro_common::error::Result<Vec<Constraint>> {
        let mut result = Vec::with_capacity(constraints.len());
        for wal_constraint in constraints {
            let constraint_type = wal_constraint.constraint_type_enum()?;
            let columns = Self::decode_constraint_columns(
                &wal_constraint.columns,
                column_count,
                constraint_type,
            )?;
            let decoded = match constraint_type {
                WalConstraintType::NotNull => {
                    if columns.len() != 1 {
                        return Err(paro_error::serialization_error(format!(
                            "WAL NOT NULL constraint expects exactly one column, got {}",
                            columns.len()
                        )));
                    }
                    Constraint::not_null(columns[0])
                }
                WalConstraintType::Unique => Constraint::unique(columns),
                WalConstraintType::PrimaryKey => {
                    if columns.is_empty() {
                        return Err(paro_error::serialization_error(
                            "WAL PRIMARY KEY constraint requires at least one column",
                        ));
                    }
                    Constraint::primary_key(columns)
                }
                WalConstraintType::ForeignKey => {
                    let referenced_table =
                        wal_constraint.referenced_table.clone().ok_or_else(|| {
                            paro_error::serialization_error(
                                "WAL FOREIGN KEY constraint missing referenced table",
                            )
                        })?;
                    let referenced_columns = wal_constraint
                        .referenced_columns
                        .as_ref()
                        .map(|values| {
                            Self::decode_constraint_columns(
                                values,
                                column_count,
                                WalConstraintType::ForeignKey,
                            )
                        })
                        .transpose()?
                        .unwrap_or_default();
                    Constraint::foreign_key(columns, referenced_table, referenced_columns)
                }
                WalConstraintType::Check => {
                    let expression = wal_constraint.expression.clone().ok_or_else(|| {
                        paro_error::serialization_error("WAL CHECK constraint missing expression")
                    })?;
                    Constraint::check(expression)
                }
            };
            result.push(decoded);
        }
        Ok(result)
    }

    pub(in crate::recovery) fn table_storage_descriptor_from_typed(
        descriptor: &DdlStorageDescriptor,
    ) -> paro_common::error::Result<TableStorageDescriptor> {
        TableStorageDescriptor::new(
            descriptor.tablet_id,
            descriptor.table_id,
            descriptor.partition_id,
            descriptor.schema_id,
            descriptor.schema_version,
            descriptor.schema_hash,
            descriptor.data_dir.clone(),
            descriptor.keys_type,
        )
    }
}
