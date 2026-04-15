// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

use crate::binder::ir::BoundStatementKind;
use crate::binder::Binder;
use paro_catalog::entry::CreateSequenceInfo;
use paro_common::error::{self as paro_error, Result};
use paro_parser::ast::{CreateOption, CreateSequenceStmt};

#[derive(Debug, Clone)]
pub struct BoundCreateSequenceInfo {
    pub database_name: String,
    pub schema_name: String,
    pub sequence_name: String,
    pub if_not_exists: bool,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub start_value: i64,
    pub cycle: bool,
}

impl BoundCreateSequenceInfo {
    pub fn to_create_sequence_info(self) -> CreateSequenceInfo {
        let mut info = CreateSequenceInfo::new(self.schema_name, self.sequence_name)
            .with_catalog(self.database_name)
            .with_increment(self.increment)
            .with_min_value(self.min_value)
            .with_max_value(self.max_value)
            .with_start_value(self.start_value);
        if self.cycle {
            info = info.with_cycle();
        }
        if self.if_not_exists {
            info = info.with_if_not_exists();
        }
        info
    }
}

pub fn bind_create_sequence(
    binder: &mut Binder,
    stmt: CreateSequenceStmt,
) -> Result<BoundStatementKind> {
    if stmt.comment.is_some() {
        return Err(paro_error::not_implemented(
            "CREATE SEQUENCE COMMENT is not yet supported",
        ));
    }

    let if_not_exists = match stmt.create_option {
        CreateOption::Create => false,
        CreateOption::CreateIfNotExists => true,
        CreateOption::CreateOrReplace => {
            return Err(paro_error::not_implemented(
                "CREATE OR REPLACE SEQUENCE is not yet supported",
            ))
        }
    };

    let schema_name = binder.session_context().current_schema().to_string();
    let database_name = binder.catalog().name().to_string();
    let sequence_name = stmt.sequence.name.clone();
    let defaults = CreateSequenceInfo::new(schema_name.clone(), sequence_name.clone());

    let start_value = stmt
        .start
        .map(i64::try_from)
        .transpose()
        .map_err(|_| paro_error::invalid_input("sequence START is out of range"))?
        .unwrap_or(defaults.start_value);
    let increment = stmt
        .increment
        .map(i64::try_from)
        .transpose()
        .map_err(|_| paro_error::invalid_input("sequence INCREMENT is out of range"))?
        .unwrap_or(defaults.increment);

    Ok(BoundStatementKind::CreateSequence(
        BoundCreateSequenceInfo {
            database_name,
            schema_name,
            sequence_name,
            if_not_exists,
            increment,
            min_value: defaults.min_value,
            max_value: defaults.max_value,
            start_value,
            cycle: defaults.cycle,
        },
    ))
}
