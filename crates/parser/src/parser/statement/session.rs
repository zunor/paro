// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::subexpr;
use crate::parser::input::Input;
use crate::parser::query::query;
use crate::parser::statement::dispatch::role_name;
use crate::parser::statement::helpers::show_options;
use crate::parser::token::Token;
use crate::parser::token::TokenKind;
use crate::parser::token::TokenKind::*;
use crate::parser::ErrorKind;

pub(crate) fn show_settings(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ SETTINGS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowSettings { show_options },
    )
    .parse(i)
}

pub(crate) fn show_variables(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ VARIABLES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowVariables { show_options },
    )
    .parse(i)
}

pub(crate) fn use_warehouse(i: Input) -> IResult<Statement> {
    map(
        rule! {
            USE ~ WAREHOUSE ~ #ident
        },
        |(_, _, warehouse)| Statement::UseWarehouse(UseWarehouseStmt { warehouse }),
    )
    .parse(i)
}

pub(crate) fn set_role(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SET ~ DEFAULT? ~ ROLE ~ #role_name
        },
        |(_, opt_is_default, _, role_name): (_, Option<&Token>, _, _)| Statement::SetRole {
            is_default: opt_is_default.is_some(),
            role_name,
        },
    )
    .parse(i)
}

pub(crate) fn set_secondary_roles(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SET ~ SECONDARY ~ ROLES ~ (ALL | NONE)
        },
        |(_, _, _, token)| {
            let option = match token.kind {
                TokenKind::ALL => SecondaryRolesOption::All,
                TokenKind::NONE => SecondaryRolesOption::None,
                _ => unreachable!(),
            };
            Statement::SetSecondaryRoles { option }
        },
    )
    .parse(i)
}

pub(crate) fn set_secondary_specify_roles(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SET ~ SECONDARY ~ ROLES ~ #comma_separated_list1(role_name)
        },
        |(_, _, _, roles)| Statement::SetSecondaryRoles {
            option: SecondaryRolesOption::SpecifyRole(roles),
        },
    )
    .parse(i)
}

pub(crate) fn set_stmt(i: Input) -> IResult<Statement> {
    alt((
        map(
            rule! {
                SET ~ #set_type ~ #ident ~ "=" ~ #subexpr(0)
            },
            |(_, set_type, var, _, value)| {
                Statement::VariableSet(VariableSetStmt {
                    kind: VariableSetKind::Set,
                    settings: Settings {
                        set_type,
                        identifiers: vec![var],
                        values: SetValues::Expr(vec![Box::new(value)]),
                    },
                })
            },
        ),
        map_res(
            rule! {
                SET ~ #set_type ~ "(" ~ #comma_separated_list0(ident) ~ ")" ~ "="
                ~ "(" ~ #comma_separated_list0(subexpr(0)) ~ ")"
            },
            |(_, set_type, _, ids, _, _, _, values, _)| {
                if ids.len() == values.len() {
                    Ok(Statement::VariableSet(VariableSetStmt {
                        kind: VariableSetKind::Set,
                        settings: Settings {
                            set_type,
                            identifiers: ids,
                            values: SetValues::Expr(values.into_iter().map(Into::into).collect()),
                        },
                    }))
                } else {
                    Err(nom::Err::Failure(ErrorKind::Other(
                        "inconsistent number of variables and values",
                    )))
                }
            },
        ),
        map(
            rule! {
                SET ~ #set_type ~ #ident ~ "=" ~ #query
            },
            |(_, set_type, var, _, query)| {
                Statement::VariableSet(VariableSetStmt {
                    kind: VariableSetKind::Set,
                    settings: Settings {
                        set_type,
                        identifiers: vec![var],
                        values: SetValues::Query(Box::new(query)),
                    },
                })
            },
        ),
        map(
            rule! {
                SET ~ #set_type ~ "(" ~ #comma_separated_list0(ident) ~ ")" ~ "=" ~ #query
            },
            |(_, set_type, _, vars, _, _, query)| {
                Statement::VariableSet(VariableSetStmt {
                    kind: VariableSetKind::Set,
                    settings: Settings {
                        set_type,
                        identifiers: vars,
                        values: SetValues::Query(Box::new(query)),
                    },
                })
            },
        ),
    ))
    .parse(i)
}

pub(crate) fn unset_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            UNSET ~ #set_type ~ #unset_source
        },
        |(_, unset_type, identifiers)| {
            Statement::VariableSet(VariableSetStmt {
                kind: VariableSetKind::Reset,
                settings: Settings {
                    set_type: unset_type,
                    identifiers,
                    values: SetValues::None,
                },
            })
        },
    )
    .parse(i)
}

pub(crate) fn reset_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            RESET ~ #set_type ~ #unset_source
        },
        |(_, unset_type, identifiers)| {
            let kind = if identifiers.is_empty() {
                VariableSetKind::ResetAll
            } else {
                VariableSetKind::Reset
            };
            Statement::VariableSet(VariableSetStmt {
                kind,
                settings: Settings {
                    set_type: unset_type,
                    identifiers,
                    values: SetValues::None,
                },
            })
        },
    )
    .parse(i)
}

pub(crate) fn unset_source(i: Input) -> IResult<Vec<Identifier>> {
    let var = map(
        rule! {
            #ident
        },
        |variable| vec![variable],
    );
    let vars = map(
        rule! {
            "(" ~ ^#comma_separated_list1(ident) ~ ")"
        },
        |(_, variables, _)| variables,
    );

    rule!(
        #var
        | #vars
    )
    .parse(i)
}
