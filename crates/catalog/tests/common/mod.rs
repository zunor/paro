//! Shared helpers for `paro-catalog` integration tests.
//!
//! Each integration test binary is a separate crate; not every test uses every helper.

#![allow(dead_code)]

use paro_common::types::LogicalType;
use paro_storage::table::table_factory::TableFactory;
use paro_storage::table::table_handle::{TableColumnSpec, TableHandle};

pub fn create_table(types: &[LogicalType]) -> TableHandle {
    TableFactory::default()
        .create_table(types)
        .expect("TableFactory::create_table")
}

pub fn create_table_from_specs(specs: &[TableColumnSpec]) -> TableHandle {
    TableFactory::default()
        .create_table_from_specs(specs)
        .expect("TableFactory::create_table_from_specs")
}
