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

use crate::ast::Hint;
use crate::ast::HintItem;
use crate::parser::common::error_hint;
use crate::parser::common::not;
use crate::parser::common::IResult;
use crate::parser::common::*;
use crate::parser::expr::literal_u64;
use crate::parser::expr::subexpr;
use crate::parser::input::Input;
use crate::parser::token::TokenKind::*;

fn set_var_hints(i: Input) -> IResult<HintItem> {
    let set_var_syntax = map(
        rule! {
            SET_VAR ~ ^"(" ~ ^#ident ~ ^"=" ~ #subexpr(0) ~ ^")"
        },
        |(_, _, name, _, expr, _)| HintItem { name, expr },
    );
    let function_style = map(
        rule! {
            #ident ~ ^"(" ~ #subexpr(0) ~ ^")"
        },
        |(name, _, expr, _)| HintItem { name, expr },
    );
    rule!(#set_var_syntax | #function_style).parse(i)
}

pub(crate) fn hint(i: Input) -> IResult<Hint> {
    let hint = map(
        rule! {
            "/*+" ~ #set_var_hints+ ~ "*/"
        },
        |(_, hints_list, _)| Hint { hints_list },
    );
    let invalid_hint = map(
        rule! {
            "/*+" ~ (!"*/" ~ #any_token)* ~ "*/"
        },
        |_| Hint { hints_list: vec![] },
    );
    rule!(#hint | #invalid_hint).parse(i)
}

pub(crate) fn top_n(i: Input) -> IResult<u64> {
    map(
        rule! {
            TOP
            ~ ^#error_hint(
                not(literal_u64),
                "expecting a literal number after keyword `TOP`, if you were referring to a column with name `top`, \
                        please quote it like `\"top\"`"
            )
            ~ ^#literal_u64
            : "TOP <limit>"
        },
        |(_, _, n)| n,
    )
    .parse(i)
}
