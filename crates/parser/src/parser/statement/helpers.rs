// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::TableRef;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::input::Input;
use crate::parser::shared::table::with_options;

pub(crate) use super::dispatch::create_table_source;
pub(crate) use super::dispatch::parse_create_option;
pub(crate) use super::dispatch::show_limit;
pub(crate) use super::dispatch::show_options;
pub(crate) use super::dispatch::table_option;
pub(crate) use super::dispatch::task_warehouse_option;

pub(crate) fn table_ref(i: Input) -> IResult<TableRef> {
    map(
        rule! {
           #dot_separated_idents_1_to_3 ~ #with_options?
        },
        |((database, schema, table), with_options)| TableRef {
            database,
            schema,
            table,
            with_options,
        },
    )
    .parse(i)
}
