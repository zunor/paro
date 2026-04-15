// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use nom::Parser;
use nom_rule::rule;

use crate::ast::CreateDynamicTableStmt;
use crate::ast::InitializeMode;
use crate::ast::RefreshMode;
use crate::ast::Statement;
use crate::ast::TargetLag;
use crate::ast::WarehouseOptions;
use crate::parser::common::IResult;
use crate::parser::Input;

use crate::parser::common::dot_separated_idents_1_to_3;
use crate::parser::common::map_res;
use crate::parser::common::*;
use crate::parser::expr::literal_u64;
use crate::parser::query::query;
use crate::parser::statement::helpers::create_table_source;
use crate::parser::statement::helpers::parse_create_option;
use crate::parser::statement::helpers::table_option;
use crate::parser::statement::helpers::task_warehouse_option;
use crate::parser::token::TokenKind::*;

pub fn dynamic_table(i: Input) -> IResult<Statement> {
    rule!(
        #create_dynamic_table : "`CREATE [OR REPLACE] [TRANSIENT] DYNAMIC TABLE [ IF NOT EXISTS ] [<database>.]<table> [<source>]
  [ CLUSTER BY <expr> ]
  TARGET_LAG = { <num> { SECOND | MINUTE | HOUR | DAY } | DOWNSTREAM}
  [ { WAREHOUSE = <string> } ]
  [ REFRESH_MODE = { AUTO | FULL | INCREMENTAL } ]
  [ INITIALIZE = { ON_CREATE | ON_SCHEDULE } ]
  [ COMMENT = '<string_literal>' ]
AS
  <sql>`"
    ).parse(i)
}

fn create_dynamic_table(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ DYNAMIC ~ TABLE ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ #create_table_source?
            ~ #dynamic_table_options
            ~ (#table_option)?
            ~ (AS ~ ^#query)
        },
        |(
            _,
            opt_or_replace,
            _,
            _,
            opt_if_not_exists,
            (database, schema, table),
            source,
            (target_lag, warehouse_opts, refresh_mode_opt, initialize_opt),
            opt_table_options,
            (_, query),
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateDynamicTable(CreateDynamicTableStmt {
                create_option,
                database,
                schema,
                table,
                source,
                target_lag,
                warehouse_opts,
                refresh_mode: refresh_mode_opt.unwrap_or(RefreshMode::Auto),
                initialize: initialize_opt.unwrap_or(InitializeMode::OnCreate),
                table_options: opt_table_options.unwrap_or_default(),
                as_query: Box::new(query),
            }))
        },
    )(i)
}

fn dynamic_table_options(
    i: Input,
) -> IResult<(
    TargetLag,
    WarehouseOptions,
    Option<RefreshMode>,
    Option<InitializeMode>,
)> {
    let target_lag = map(
        rule! {
            TARGET_LAG ~ "=" ~ #target_lag
        },
        |(_, _, target_lag)| target_lag,
    );

    let refresh_mode = alt((
        value(RefreshMode::Auto, rule! { AUTO }),
        value(RefreshMode::Full, rule! { FULL }),
        value(RefreshMode::Incremental, rule! { INCREMENTAL }),
    ));
    let refresh_mode_opt = map(
        rule! {
            (REFRESH_MODE ~ "=" ~ #refresh_mode)?
        },
        |v| v.map(|v| v.2),
    );

    let initialize_mode = alt((
        value(InitializeMode::OnCreate, rule! { ON_CREATE }),
        value(InitializeMode::OnSchedule, rule! { ON_SCHEDULE }),
    ));
    let initialize_opt = map(
        rule! {
            (INITIALIZE ~ "=" ~ #initialize_mode)?
        },
        |v| v.map(|v| v.2),
    );

    permutation((
        target_lag,
        task_warehouse_option,
        refresh_mode_opt,
        initialize_opt,
    ))
    .parse(i)
}

fn target_lag(i: Input) -> IResult<TargetLag> {
    let interval_sec = map(
        rule! {
             #literal_u64 ~ SECOND
        },
        |(secs, _)| TargetLag::IntervalSecs(secs),
    );
    let interval_min = map(
        rule! {
             #literal_u64 ~ MINUTE
        },
        |(mins, _)| TargetLag::IntervalSecs(mins * 60),
    );
    let interval_hour = map(
        rule! {
             #literal_u64 ~ HOUR
        },
        |(hours, _)| TargetLag::IntervalSecs(hours * 60 * 60),
    );
    let interval_day = map(
        rule! {
             #literal_u64 ~ DAY
        },
        |(days, _)| TargetLag::IntervalSecs(days * 60 * 60 * 24),
    );
    let downstream = map(
        rule! {
            DOWNSTREAM
        },
        |_| TargetLag::Downstream,
    );
    rule!(
        #interval_sec
        | #interval_min
        | #interval_hour
        | #interval_day
        | #downstream
    )
    .parse(i)
}
