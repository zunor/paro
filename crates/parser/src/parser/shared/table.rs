// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::collections::BTreeMap;

use nom::Parser;
use nom_rule::rule;

use crate::ast::WithOptions;
use crate::parser::common::comma_separated_list1;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::expr::literal_bool;
use crate::parser::input::Input;
use crate::parser::statement::stage::parameter_to_string;
use crate::parser::token::TokenKind::*;

fn option_to_string(i: Input) -> IResult<String> {
    let bool_to_string = |i| map(literal_bool, |value| value.to_string()).parse(i);

    rule!(
        #bool_to_string
        | #parameter_to_string
    )
    .parse(i)
}

pub(crate) fn set_table_option(i: Input) -> IResult<BTreeMap<String, String>> {
    let option = map(
        rule! {
           #ident ~ "=" ~ #option_to_string
        },
        |(key, _, value)| (key, value),
    );

    map(comma_separated_list1(option), |options| {
        options
            .into_iter()
            .map(|(key, value)| (key.name.to_lowercase(), value))
            .collect()
    })
    .parse(i)
}

pub(crate) fn with_options(i: Input) -> IResult<WithOptions> {
    alt((
        map(rule! { WITH ~ CONSUME }, |_| WithOptions {
            options: BTreeMap::from([("consume".to_string(), "true".to_string())]),
        }),
        map(
            rule! {
                WITH ~ "(" ~ #set_table_option ~ ")"
            },
            |(_, _, options, _)| WithOptions { options },
        ),
    ))
    .parse(i)
}
