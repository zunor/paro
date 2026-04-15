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

use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::literal_i64;
use crate::parser::expr::literal_string;
use crate::parser::expr::subexpr;
use crate::parser::expr::type_name;
use crate::parser::input::Input;
use crate::parser::query::query;
use crate::parser::statement::dml::insert_stmt;
use crate::parser::token::TokenKind;
use crate::parser::token::TokenKind::*;

fn query_statement(i: Input) -> IResult<Statement> {
    map(query, |query| Statement::Query(Box::new(query))).parse(i)
}

pub(crate) fn transaction_stmt(i: Input) -> IResult<Statement> {
    alt((
        value(
            Statement::Transaction(TransactionStmt {
                kind: TransactionKind::Begin,
            }),
            rule! { BEGIN ~ TRANSACTION? },
        ),
        value(
            Statement::Transaction(TransactionStmt {
                kind: TransactionKind::Start,
            }),
            rule! { START ~ TRANSACTION },
        ),
        map(
            rule! { COMMIT ~ PREPARED ~ #literal_string },
            |(_, _, gid)| {
                Statement::Transaction(TransactionStmt {
                    kind: TransactionKind::CommitPrepared(gid),
                })
            },
        ),
        map(
            rule! { PREPARE ~ TRANSACTION ~ #literal_string },
            |(_, _, gid)| {
                Statement::Transaction(TransactionStmt {
                    kind: TransactionKind::PrepareTransaction(gid),
                })
            },
        ),
        map(rule! { SAVEPOINT ~ #ident }, |(_, name)| {
            Statement::Transaction(TransactionStmt {
                kind: TransactionKind::Savepoint(name),
            })
        }),
        map(rule! { RELEASE ~ SAVEPOINT ~ #ident }, |(_, _, name)| {
            Statement::Transaction(TransactionStmt {
                kind: TransactionKind::ReleaseSavepoint(name),
            })
        }),
        map(rule! { RELEASE ~ #ident }, |(_, name)| {
            Statement::Transaction(TransactionStmt {
                kind: TransactionKind::ReleaseSavepoint(name),
            })
        }),
        map(
            rule! { ROLLBACK ~ TO ~ SAVEPOINT ~ #ident },
            |(_, _, _, name)| {
                Statement::Transaction(TransactionStmt {
                    kind: TransactionKind::RollbackToSavepoint(name),
                })
            },
        ),
        map(rule! { ROLLBACK ~ TO ~ #ident }, |(_, _, name)| {
            Statement::Transaction(TransactionStmt {
                kind: TransactionKind::RollbackToSavepoint(name),
            })
        }),
        map(
            rule! { ROLLBACK ~ PREPARED ~ #literal_string },
            |(_, _, gid)| {
                Statement::Transaction(TransactionStmt {
                    kind: TransactionKind::RollbackPrepared(gid),
                })
            },
        ),
        value(
            Statement::Transaction(TransactionStmt {
                kind: TransactionKind::Commit,
            }),
            rule! { COMMIT },
        ),
        value(
            Statement::Transaction(TransactionStmt {
                kind: TransactionKind::Rollback,
            }),
            rule! { ABORT | ROLLBACK },
        ),
    ))
    .parse(i)
}

pub(crate) fn prepare_stmt(i: Input) -> IResult<Statement> {
    let prep_type_clause = map(
        rule! {
            "(" ~ #comma_separated_list1(type_name) ~ ")"
        },
        |(_, types, _)| types,
    );
    let preparable_stmt = rule!(
        #query_statement
        | #insert_stmt(false, false)
    );

    map(
        rule! {
            PREPARE ~ #ident ~ #prep_type_clause? ~ AS ~ #preparable_stmt
        },
        |(_, name, opt_types, _, statement)| {
            Statement::Prepare(PrepareStmt {
                name,
                parameter_types: opt_types.unwrap_or_default(),
                statement: Box::new(statement),
            })
        },
    )
    .parse(i)
}

pub(crate) fn execute_prepared_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            EXECUTE ~ #ident ~ ( "(" ~ #comma_separated_list0(subexpr(0)) ~ ")" )?
        },
        |(_, name, opt_args)| {
            Statement::Execute(ExecuteStmt {
                name,
                args: opt_args
                    .map(|(_, args, _)| args.into_iter().map(Box::new).collect())
                    .unwrap_or_default(),
            })
        },
    )
    .parse(i)
}

pub(crate) fn deallocate_stmt(i: Input) -> IResult<Statement> {
    alt((
        map(rule! { DEALLOCATE ~ ALL }, |_| {
            Statement::Deallocate(DeallocateStmt { name: None })
        }),
        map(rule! { DEALLOCATE ~ PREPARE ~ ALL }, |_| {
            Statement::Deallocate(DeallocateStmt { name: None })
        }),
        map(rule! { DEALLOCATE ~ #ident }, |(_, name)| {
            Statement::Deallocate(DeallocateStmt { name: Some(name) })
        }),
        map(rule! { DEALLOCATE ~ PREPARE ~ #ident }, |(_, _, name)| {
            Statement::Deallocate(DeallocateStmt { name: Some(name) })
        }),
    ))
    .parse(i)
}

pub(crate) fn declare_cursor_stmt(i: Input) -> IResult<Statement> {
    let cursor_scroll_mode = alt((
        value(CursorScrollMode::Scroll, rule! { SCROLL }),
        value(CursorScrollMode::NoScroll, rule! { NO ~ SCROLL }),
    ));

    map(
        rule! {
            DECLARE ~ #ident ~ #cursor_scroll_mode? ~ CURSOR ~ (WITH ~ HOLD)? ~ FOR ~ #query_statement
        },
        |(_, name, scroll, _, opt_hold, _, query)| {
            Statement::DeclareCursor(DeclareCursorStmt {
                name,
                scroll: scroll.unwrap_or(CursorScrollMode::Unspecified),
                hold: opt_hold.is_some(),
                query: Box::new(query),
            })
        },
    )
    .parse(i)
}

pub(crate) fn fetch_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            FETCH ~ #fetch_direction? ~ (FROM | IN) ~ #ident
        },
        |(_, direction, _, cursor)| {
            Statement::Fetch(FetchStmt {
                ismove: false,
                direction: direction.unwrap_or(FetchDirection::Next),
                cursor,
            })
        },
    )
    .parse(i)
}

pub(crate) fn move_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            MOVE ~ #fetch_direction? ~ (FROM | IN) ~ #ident
        },
        |(_, direction, _, cursor)| {
            Statement::Fetch(FetchStmt {
                ismove: true,
                direction: direction.unwrap_or(FetchDirection::Next),
                cursor,
            })
        },
    )
    .parse(i)
}

pub(crate) fn close_cursor_stmt(i: Input) -> IResult<Statement> {
    alt((
        map(rule! { CLOSE ~ ALL }, |_| {
            Statement::CloseCursor(CloseCursorStmt { name: None })
        }),
        map(rule! { CLOSE ~ #ident }, |(_, name)| {
            Statement::CloseCursor(CloseCursorStmt { name: Some(name) })
        }),
    ))
    .parse(i)
}

pub(crate) fn discard_stmt(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DISCARD ~ (ALL | TEMPORARY | TEMP | PLANS | SEQUENCES)
        },
        |(_, target)| {
            let target = match target.kind {
                TokenKind::ALL => DiscardTarget::All,
                TokenKind::TEMPORARY | TokenKind::TEMP => DiscardTarget::Temp,
                TokenKind::PLANS => DiscardTarget::Plans,
                TokenKind::SEQUENCES => DiscardTarget::Sequences,
                _ => unreachable!(),
            };
            Statement::Discard(DiscardStmt { target })
        },
    )
    .parse(i)
}

pub(crate) fn checkpoint_stmt(i: Input) -> IResult<Statement> {
    value(Statement::Checkpoint(CheckpointStmt), rule! { CHECKPOINT }).parse(i)
}

pub(crate) fn fetch_direction(i: Input) -> IResult<FetchDirection> {
    alt((
        value(FetchDirection::ForwardAll, rule! { ALL }),
        value(FetchDirection::Next, rule! { NEXT }),
        value(FetchDirection::Prior, rule! { PRIOR }),
        value(FetchDirection::First, rule! { FIRST }),
        value(FetchDirection::Last, rule! { LAST }),
        value(FetchDirection::ForwardAll, rule! { FORWARD ~ ALL }),
        value(FetchDirection::BackwardAll, rule! { BACKWARD ~ ALL }),
        map(rule! { FORWARD ~ #literal_i64 }, |(_, count)| {
            FetchDirection::ForwardCount(count)
        }),
        map(rule! { BACKWARD ~ #literal_i64 }, |(_, count)| {
            FetchDirection::BackwardCount(count)
        }),
        map(rule! { ABSOLUTE ~ #literal_i64 }, |(_, count)| {
            FetchDirection::Absolute(count)
        }),
        map(rule! { RELATIVE ~ #literal_i64 }, |(_, count)| {
            FetchDirection::Relative(count)
        }),
        map(rule! { #literal_i64 }, FetchDirection::Count),
    ))
    .parse(i)
}
