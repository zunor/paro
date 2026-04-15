//! Physical Drop Operator
//!
//!
//! ## Dependencies Check
//! - Allocator: N/A (DDL operation)
//! - BufferManager: N/A (DDL operation)
//!
//! ## Known Limitations
//! - CASCADE only implemented for DROP SCHEMA
//! - No dependency checking for indexes

use crate::execution_context::ExecutionContext;
use crate::operator::state::OperatorSourceInput;
use crate::operator::PhysicalOperator;
use crate::operator_type::PhysicalOperatorType;
use crate::result_type::SourceResultType;
use paro_catalog::entry::{CatalogEntryEnum, CatalogType, DropEntryInfo};
use paro_common::chunk::Chunk;
use paro_common::error::{self as paro_error, Result};
use paro_common::types::LogicalType;
use paro_planner::binder::ir::statement::{BoundDropInfo, DropType};
use std::any::Any;

/// Drop represents a DROP operation (TABLE, SCHEMA, VIEW, etc.).
///
/// This is a Source operator that performs the drop operation and returns
/// an empty result.
#[derive(Debug)]
pub struct Drop {
    pub info: BoundDropInfo,
}

impl Drop {
    pub fn new(info: BoundDropInfo) -> Self {
        Self { info }
    }
}

impl PhysicalOperator for Drop {
    fn operator_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::Drop
    }

    fn types(&self) -> &[LogicalType] {
        &[]
    }

    fn is_source(&self) -> bool {
        true
    }

    fn get_data(
        &self,
        ctx: &ExecutionContext,
        _chunk: &mut Chunk,
        _input: &mut OperatorSourceInput,
    ) -> Result<SourceResultType> {
        // Get catalog from session
        let catalog = ctx.session.catalog();
        let ddl = ctx
            .session
            .ddl()
            .expect("ddl context must exist inside transactions");

        // TODO: Support cross-database drop
        if catalog.name() != self.info.database_name {
            return Err(paro_error::catalog(format!(
                "Database mismatch: expected {}, got {}",
                self.info.database_name,
                catalog.name()
            )));
        }

        let txn = ctx.session.catalog_txn_view();

        match self.info.drop_type {
            DropType::Table => {
                ddl.apply_drop(
                    self.info.schema_name.clone(),
                    if self.info.if_exists {
                        DropEntryInfo::new(CatalogType::Table, self.info.object_name.clone())
                            .with_if_exists()
                    } else {
                        DropEntryInfo::new(CatalogType::Table, self.info.object_name.clone())
                    },
                )?;
            }
            DropType::Schema => {
                if self.info.if_exists && catalog.get_schema(&txn, &self.info.object_name).is_err()
                {
                    return Ok(SourceResultType::Finished);
                }
                let mut drop_info =
                    DropEntryInfo::new(CatalogType::Schema, self.info.object_name.clone());
                if self.info.if_exists {
                    drop_info = drop_info.with_if_exists();
                }
                if self.info.cascade {
                    drop_info = drop_info.with_cascade();
                }
                ddl.apply_drop(self.info.object_name.clone(), drop_info)?;
            }
            DropType::Index => {
                let schema = catalog.get_schema(&txn, &self.info.schema_name)?;
                let existing =
                    schema.get_index(txn.transaction_id, txn.start_time, &self.info.object_name);

                let Some(existing_entry) = existing else {
                    if self.info.if_exists {
                        return Ok(SourceResultType::Finished);
                    }
                    return Err(paro_error::object_not_found(
                        "index",
                        &self.info.object_name,
                    ));
                };

                let CatalogEntryEnum::Index(_) = existing_entry.as_ref() else {
                    return Err(paro_error::wrong_object_type(
                        "index",
                        &self.info.object_name,
                    ));
                };

                let drop_info = if self.info.if_exists {
                    DropEntryInfo::new(CatalogType::Index, self.info.object_name.clone())
                        .with_if_exists()
                } else {
                    DropEntryInfo::new(CatalogType::Index, self.info.object_name.clone())
                };
                ddl.apply_drop(self.info.schema_name.clone(), drop_info)?;
            }
            DropType::View => {
                ddl.apply_drop(
                    self.info.schema_name.clone(),
                    if self.info.if_exists {
                        DropEntryInfo::new(CatalogType::View, self.info.object_name.clone())
                            .with_if_exists()
                    } else {
                        DropEntryInfo::new(CatalogType::View, self.info.object_name.clone())
                    },
                )?;
            }
            DropType::Sequence => {
                ddl.apply_drop(
                    self.info.schema_name.clone(),
                    if self.info.if_exists {
                        DropEntryInfo::new(CatalogType::Sequence, self.info.object_name.clone())
                            .with_if_exists()
                    } else {
                        DropEntryInfo::new(CatalogType::Sequence, self.info.object_name.clone())
                    },
                )?;
            }
        }

        Ok(SourceResultType::Finished)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
