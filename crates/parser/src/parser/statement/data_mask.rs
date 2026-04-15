// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::DataMaskArg;
use crate::ast::DataMaskPolicy;
use crate::ast::Expr;
use crate::ast::TypeName;
use crate::parser::common::*;
use crate::parser::expr::*;
use crate::parser::input::Input;
use crate::parser::token::*;

fn data_mask_arg(i: Input) -> IResult<DataMaskArg> {
    map(rule! { #ident ~ #type_name }, |(arg_name, arg_type)| {
        DataMaskArg {
            arg_name: arg_name.name,
            arg_type,
        }
    })
    .parse(i)
}

fn data_mask_args(i: Input) -> IResult<Vec<DataMaskArg>> {
    map(
        rule! { AS ~ "(" ~ #comma_separated_list1(data_mask_arg) ~ ")" },
        |(_, _, args, _)| args,
    )
    .parse(i)
}

fn data_mask_body(i: Input) -> IResult<Expr> {
    map(rule! { #expr }, |expr| expr).parse(i)
}

fn data_mask_return_type(i: Input) -> IResult<TypeName> {
    map(rule! { RETURNS ~ #type_name }, |(_, type_name)| type_name).parse(i)
}

pub fn data_mask_policy(i: Input) -> IResult<DataMaskPolicy> {
    map(
        rule! { #data_mask_args ~ #data_mask_return_type ~ "->" ~ #data_mask_body ~ ( COMMENT ~ "=" ~ #literal_string)? },
        |(args, return_type, _, body, comment_opt)| DataMaskPolicy {
            args,
            return_type,
            body,
            comment: match comment_opt {
                Some(opt) => Some(opt.2),
                None => None,
            },
        },
    ).parse(i)
}
