// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::literal_string;
use crate::parser::input::Input;
use crate::parser::statement::dispatch::auth_type;
use crate::parser::statement::dispatch::grant_option;
use crate::parser::statement::dispatch::grant_ownership_level;
use crate::parser::statement::dispatch::grant_source;
use crate::parser::statement::dispatch::user_identity;
use crate::parser::statement::dispatch::user_option;
use crate::parser::statement::helpers::parse_create_option;
use crate::parser::statement::helpers::show_options;
use crate::parser::token::TokenKind::*;
use crate::parser::ErrorKind;

pub(crate) fn show_users(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ USERS ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowUsers { show_options },
    )
    .parse(i)
}

pub(crate) fn describe_user(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ( DESC | DESCRIBE ) ~ USER ~ #user_identity
        },
        |(_, _, user)| Statement::DescribeUser { user },
    )
    .parse(i)
}

pub(crate) fn create_user(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~  ( OR ~ ^REPLACE )? ~ USER ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #user_identity
            ~ IDENTIFIED ~ ( WITH ~ ^#auth_type )? ~ ( BY ~ ^#literal_string )?
            ~ ( WITH ~ ^#comma_separated_list1(user_option))?
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            user,
            _,
            opt_auth_type,
            opt_password,
            opt_user_option,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateUser(CreateUserStmt {
                create_option,
                user,
                auth_option: AuthOption {
                    auth_type: opt_auth_type.map(|(_, auth_type)| auth_type),
                    password: opt_password.map(|(_, password)| password),
                },
                user_options: opt_user_option
                    .map(|(_, user_options)| user_options)
                    .unwrap_or_default(),
            }))
        },
    )
    .parse(i)
}

pub(crate) fn alter_user(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ALTER ~ USER ~ ( #map(rule! { USER ~ "(" ~ ")" }, |_| None) | #map(user_identity, Some) )
            ~ ( IDENTIFIED ~ ( WITH ~ ^#auth_type )? ~ ( BY ~ ^#literal_string )? )?
            ~ ( WITH ~ ^#comma_separated_list1(user_option) )?
        },
        |(_, _, user, opt_auth_option, opt_user_option)| {
            Statement::AlterUser(AlterUserStmt {
                user,
                auth_option: opt_auth_option.map(|(_, opt_auth_type, opt_password)| AuthOption {
                    auth_type: opt_auth_type.map(|(_, auth_type)| auth_type),
                    password: opt_password.map(|(_, password)| password),
                }),
                user_options: opt_user_option
                    .map(|(_, user_options)| user_options)
                    .unwrap_or_default(),
            })
        },
    )
    .parse(i)
}

pub(crate) fn drop_user(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ USER ~ ( IF ~ ^EXISTS )? ~ #user_identity
        },
        |(_, _, opt_if_exists, user)| Statement::DropUser {
            if_exists: opt_if_exists.is_some(),
            user,
        },
    )
    .parse(i)
}

pub(crate) fn show_roles(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ ROLES ~ #show_options?
        },
        |(_, _, show_options)| Statement::ShowRoles { show_options },
    )
    .parse(i)
}

pub(crate) fn create_role(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ROLE ~ ( IF ~ ^NOT ~ ^EXISTS )? ~ #role_name ~ ( COMMENT ~ ^"=" ~ ^#literal_string )?
        },
        |(_, _, opt_if_not_exists, role_name, opt_comment)| {
            let create_option = parse_create_option(false, opt_if_not_exists.is_some())?;
            Ok(Statement::CreateRole {
                create_option,
                role_name,
                comment: opt_comment.map(|(_, _, comment)| comment),
            })
        },
    )
    .parse(i)
}

pub(crate) fn alter_role(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ALTER ~ ROLE ~ ( IF ~ ^EXISTS )? ~ ^#ident
             ~ #alter_role_action
        },
        |(_, _, opt_if_exists, name, action)| {
            let stmt = AlterRoleStmt {
                if_exists: opt_if_exists.is_some(),
                name: name.to_string(),
                action,
            };
            Statement::AlterRole(stmt)
        },
    )
    .parse(i)
}

pub(crate) fn drop_role(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ ROLE ~ ( IF ~ ^EXISTS )? ~ #role_name
        },
        |(_, _, opt_if_exists, role_name)| Statement::DropRole {
            if_exists: opt_if_exists.is_some(),
            role_name,
        },
    )
    .parse(i)
}

pub(crate) fn grant(i: Input) -> IResult<Statement> {
    map(
        rule! {
            GRANT ~ #grant_source ~ TO ~ #grant_option
        },
        |(_, source, _, grant_option)| {
            Statement::Grant(GrantStmt {
                source,
                principal: grant_option,
            })
        },
    )
    .parse(i)
}

pub(crate) fn grant_ownership(i: Input) -> IResult<Statement> {
    map(
        rule! {
            GRANT ~ OWNERSHIP ~ ON ~ #grant_ownership_level  ~ TO ~ ROLE ~ #role_name
        },
        |(_, _, _, level, _, _, role_name)| {
            Statement::Grant(GrantStmt {
                source: AccountMgrSource::Privs {
                    privileges: vec![UserPrivilegeType::Ownership],
                    level,
                },
                principal: PrincipalIdentity::Role(role_name),
            })
        },
    )
    .parse(i)
}

pub(crate) fn revoke(i: Input) -> IResult<Statement> {
    map(
        rule! {
            REVOKE ~ #grant_source ~ FROM ~ #grant_option
        },
        |(_, source, _, grant_option)| {
            Statement::Revoke(RevokeStmt {
                source,
                principal: grant_option,
            })
        },
    )
    .parse(i)
}

fn alter_role_action(i: Input) -> IResult<AlterRoleAction> {
    let set_comment = map(
        rule! {
           SET ~ COMMENT ~ ^"=" ~ ^#literal_string
        },
        |(_, _, _, comment)| AlterRoleAction::Comment(Some(comment)),
    );
    let unset_comment = value(AlterRoleAction::Comment(None), rule! { UNSET ~ COMMENT });

    rule!(
        #set_comment
        | #unset_comment
    )
    .parse(i)
}

pub(crate) fn role_name(i: Input) -> IResult<String> {
    let role_ident = map_res(
        rule! {
            #ident
        },
        |role_name| {
            let name = role_name.name;
            let mut chars = name.chars();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => match chars.next() {
                        Some('f') | Some('b') => {
                            return Err(nom::Err::Failure(ErrorKind::Other(
                                "' or \" or \\f or \\b are not allowed in role name",
                            )));
                        }
                        _ => {}
                    },
                    '\'' | '"' => {
                        return Err(nom::Err::Failure(ErrorKind::Other(
                            "' or \" or \\f or \\b are not allowed in role name",
                        )));
                    }
                    _ => {}
                }
            }
            Ok(name)
        },
    );
    let role_lit = map(rule! { #literal_string }, |role_name| role_name);

    rule!(
        #role_ident : "<role_name>"
        | #role_lit : "'<role_name>'"
    )
    .parse(i)
}
