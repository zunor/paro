// Copyright 2024-2026 Zunor
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
