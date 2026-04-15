// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use std::time::Duration;

use nom::Parser;
use nom_rule::rule;

use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::expr;
use crate::parser::expr::literal_string;
use crate::parser::expr::literal_u64;
use crate::parser::input::Input;
use crate::parser::statement::stage::parameter_to_string;
use crate::parser::statement::stage::stage_location;
use crate::parser::token::TokenKind::*;

pub(crate) fn kill_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            KILL ~ #kill_target ~ #parameter_to_string
        },
        |(_, kill_target, object_id)| Statement::KillStmt {
            kill_target,
            object_id,
        },
    )
    .parse(i)
}

pub(crate) fn set_priority(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SET ~ PRIORITY ~ #priority ~ #parameter_to_string
        },
        |(_, _, priority, object_id)| Statement::SetPriority {
            object_id,
            priority,
        },
    )
    .parse(i)
}

pub(crate) fn execute_immediate(i: Input) -> IResult<Statement> {
    map(
        rule! {
            EXECUTE ~ IMMEDIATE ~ #expr
        },
        |(_, _, script)| Statement::ExecuteImmediate(ExecuteImmediateStmt { script }),
    )
    .parse(i)
}

pub(crate) fn system_action(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SYSTEM ~ #action
        },
        |(_, action)| Statement::System(SystemStmt { action }),
    )
    .parse(i)
}

pub(crate) fn presign(i: Input) -> IResult<Statement> {
    map(
        rule! {
            PRESIGN ~ ( #presign_action )?
                ~ #presign_location
                ~ ( #presign_option )*
        },
        |(_, action, location, opts)| {
            let mut presign_stmt = PresignStmt {
                action: action.unwrap_or_default(),
                location,
                expire: Duration::from_secs(3600),
                content_type: None,
            };
            for opt in opts {
                presign_stmt.apply_option(opt);
            }
            Statement::Presign(presign_stmt)
        },
    )
    .parse(i)
}

pub(crate) fn kill_target(i: Input) -> IResult<KillTarget> {
    alt((
        value(KillTarget::Query, rule! { QUERY }),
        value(KillTarget::Connection, rule! { CONNECTION }),
    ))
    .parse(i)
}

pub(crate) fn priority(i: Input) -> IResult<Priority> {
    alt((
        value(Priority::LOW, rule! { LOW }),
        value(Priority::MEDIUM, rule! { MEDIUM }),
        value(Priority::HIGH, rule! { HIGH }),
    ))
    .parse(i)
}

pub(crate) fn action(i: Input) -> IResult<SystemAction> {
    let backtrace = parser_fn(map(
        rule! {
             #switch ~ EXCEPTION_BACKTRACE
        },
        |(switch, _)| SystemAction::Backtrace(switch),
    ));
    let flush_privileges = parser_fn(map(
        rule! {
             FLUSH ~ PRIVILEGES
        },
        |_| SystemAction::FlushPrivileges,
    ));

    rule!(
        #backtrace | #flush_privileges
    )
    .parse(i)
}

pub(crate) fn switch(i: Input) -> IResult<bool> {
    alt((
        value(true, rule! { ENABLE }),
        value(false, rule! { DISABLE }),
    ))
    .parse(i)
}

pub(crate) fn presign_action(i: Input) -> IResult<PresignAction> {
    alt((
        value(PresignAction::Download, rule! { DOWNLOAD }),
        value(PresignAction::Upload, rule! { UPLOAD }),
    ))
    .parse(i)
}

pub(crate) fn presign_location(i: Input) -> IResult<PresignLocation> {
    map_res(rule! { #stage_location }, |v| {
        Ok(PresignLocation::StageLocation(v))
    })
    .parse(i)
}

pub(crate) fn presign_option(i: Input) -> IResult<PresignOption> {
    alt((
        map(rule! { EXPIRE ~ ^"=" ~ ^#literal_u64 }, |(_, _, v)| {
            PresignOption::Expire(v)
        }),
        map(
            rule! { CONTENT_TYPE ~ ^"=" ~ ^#literal_string },
            |(_, _, v)| PresignOption::ContentType(v),
        ),
    ))
    .parse(i)
}
