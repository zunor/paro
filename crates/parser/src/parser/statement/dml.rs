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
use crate::parser::expr::at_string;
use crate::parser::expr::expr;
use crate::parser::input::Input;
use crate::parser::query::*;
use crate::parser::shared::hint::hint;
use crate::parser::shared::table::with_options;
use crate::parser::statement::dispatch::rest_str;
use crate::parser::statement::dispatch::table_reference_with_alias;
use crate::parser::statement::stage::file_format_clause;
use crate::parser::token::TokenKind::*;
use crate::parser::ErrorKind;

pub(crate) fn merge(i: Input) -> IResult<Statement> {
    map(
        rule! {
            MERGE ~ #hint?
            ~ INTO ~ #dot_separated_idents_1_to_3 ~ #table_alias?
            ~ USING ~ #mutation_source
            ~ ON ~ #expr ~ (#match_clause | #unmatch_clause)*
        },
        |(
            _,
            opt_hints,
            _,
            (catalog, database, table),
            target_alias,
            _,
            source,
            _,
            join_expr,
            merge_options,
        )| {
            Statement::MergeInto(MergeIntoStmt {
                hints: opt_hints,
                database: catalog,
                schema: database,
                table_ident: table,
                source,
                target_alias,
                join_expr,
                merge_options,
            })
        },
    )
    .parse(i)
}

pub(crate) fn delete(i: Input) -> IResult<Statement> {
    map(
        rule! {
            #with? ~ DELETE ~ #hint? ~ FROM ~ #table_reference_with_alias ~ ( WHERE ~ ^#expr )?
        },
        |(with, _, hints, _, table, opt_selection)| {
            Statement::Delete(DeleteStmt {
                hints,
                table,
                selection: opt_selection.map(|(_, selection)| selection),
                with,
            })
        },
    )
    .parse(i)
}

pub(crate) fn update(i: Input) -> IResult<Statement> {
    map(
        rule! {
            #with? ~ UPDATE ~ #hint? ~ #dot_separated_idents_1_to_3 ~ #table_alias?
            ~ SET ~ ^#comma_separated_list1(mutation_update_expr)
            ~ ( FROM ~ #mutation_source )?
            ~ ( WHERE ~ ^#expr )?
        },
        |(
            with,
            _,
            hints,
            (database, schema, table),
            table_alias,
            _,
            update_list,
            from,
            opt_selection,
        )| {
            Statement::Update(UpdateStmt {
                hints,
                database,
                schema,
                table,
                table_alias,
                update_list,
                from: from.map(|(_, table)| table),
                selection: opt_selection.map(|(_, selection)| selection),
                with,
            })
        },
    )
    .parse(i)
}

pub(crate) fn insert_stmt(
    allow_raw: bool,
    in_streaming_load: bool,
) -> impl FnMut(Input) -> IResult<Statement> {
    move |i| {
        let insert_source_parser = if in_streaming_load {
            insert_source_file
        } else if allow_raw {
            insert_source_fast_values
        } else {
            insert_source
        };
        let on_conflict_do_nothing = map(
            rule! {
                ON ~ CONFLICT ~ "(" ~ #comma_separated_list1(ident) ~ ")" ~ DO ~ "NOTHING"
            },
            |(_, _, _, columns, _, _, _)| OnConflictClause {
                columns,
                action: OnConflictAction::DoNothing,
            },
        );
        let on_conflict_do_update = map(
            rule! {
                ON ~ CONFLICT ~ "(" ~ #comma_separated_list1(ident) ~ ")" ~ DO ~ UPDATE ~ SET
                ~ #comma_separated_list1(mutation_update_expr)
            },
            |(_, _, _, columns, _, _, _, _, update_list)| OnConflictClause {
                columns,
                action: OnConflictAction::DoUpdate { update_list },
            },
        );
        map_res(
            rule! {
                #with? ~ INSERT ~ #hint? ~ OVERWRITE? ~ INTO? ~ TABLE?
                ~ #dot_separated_idents_1_to_3
                ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
                ~ #insert_source_parser
                ~ ( #on_conflict_do_update | #on_conflict_do_nothing )?
            },
            |(
                with,
                _,
                opt_hints,
                overwrite,
                into,
                _,
                (database, schema, table),
                opt_columns,
                source,
                on_conflict,
            )| {
                if overwrite.is_none() && into.is_none() {
                    return Err(nom::Err::Failure(ErrorKind::Other(
                        "INSERT statement must be followed by 'overwrite' or 'into'",
                    )));
                }
                Ok(Statement::Insert(InsertStmt {
                    hints: opt_hints,
                    with,
                    database,
                    schema,
                    table,
                    columns: opt_columns
                        .map(|(_, columns, _)| columns)
                        .unwrap_or_default(),
                    source,
                    on_conflict,
                    overwrite: overwrite.is_some(),
                }))
            },
        )(i)
    }
}

pub(crate) fn conditional_multi_table_insert() -> impl FnMut(Input) -> IResult<Statement> {
    move |i| {
        map(
            rule! {
                INSERT ~ OVERWRITE? ~ (FIRST | ALL) ~ (#when_clause)+ ~ (#else_clause)? ~ #query
            },
            |(_, overwrite, kind, when_clauses, opt_else, source)| {
                Statement::InsertMultiTable(InsertMultiTableStmt {
                    overwrite: overwrite.is_some(),
                    is_first: matches!(kind.kind, FIRST),
                    when_clauses,
                    else_clause: opt_else,
                    into_clauses: vec![],
                    source,
                })
            },
        )
        .parse(i)
    }
}

pub(crate) fn unconditional_multi_table_insert() -> impl FnMut(Input) -> IResult<Statement> {
    move |i| {
        map(
            rule! {
                INSERT ~ OVERWRITE? ~ ALL ~ (#into_clause)+ ~ #query
            },
            |(_, overwrite, _, into_clauses, source)| {
                Statement::InsertMultiTable(InsertMultiTableStmt {
                    overwrite: overwrite.is_some(),
                    is_first: false,
                    when_clauses: vec![],
                    else_clause: None,
                    into_clauses,
                    source,
                })
            },
        )
        .parse(i)
    }
}

pub(crate) fn replace_stmt(allow_raw: bool) -> impl FnMut(Input) -> IResult<Statement> {
    move |i| {
        let insert_source_parser = if allow_raw {
            insert_source_fast_values
        } else {
            insert_source
        };
        map(
            rule! {
                REPLACE ~ #hint? ~ INTO?
                ~ #dot_separated_idents_1_to_3
                ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
                ~ ON ~ CONFLICT? ~ "(" ~ #comma_separated_list1(ident) ~ ")"
                ~ ( DELETE ~ WHEN ~ ^#expr )?
                ~ #insert_source_parser
            },
            |(
                _,
                opt_hints,
                _,
                (database, schema, table),
                opt_columns,
                _,
                opt_conflict,
                _,
                on_conflict_columns,
                _,
                opt_delete_when,
                source,
            )| {
                Statement::Replace(ReplaceStmt {
                    hints: opt_hints,
                    database,
                    schema,
                    table,
                    is_conflict: opt_conflict.is_some(),
                    on_conflict_columns,
                    columns: opt_columns
                        .map(|(_, columns, _)| columns)
                        .unwrap_or_default(),
                    source,
                    delete_when: opt_delete_when.map(|(_, _, expr)| expr),
                })
            },
        )
        .parse(i)
    }
}

pub(crate) fn insert_source(i: Input) -> IResult<InsertSource> {
    let row = parser_fn(map(
        rule! {
            "(" ~ #comma_separated_list1(expr) ~ ")"
        },
        |(_, values, _)| values,
    ));
    let values = parser_fn(map(
        rule! {
            VALUES ~ #comma_separated_list0(row)
        },
        |(_, rows)| InsertSource::Values { rows },
    ));

    let query = parser_fn(map(query, |query| InsertSource::Select {
        query: Box::new(query),
    }));

    rule!(
        #values
        | #query
    )
    .parse(i)
}

pub(crate) fn insert_source_file(i: Input) -> IResult<InsertSource> {
    let value = map(
        rule! {
            "(" ~ #comma_separated_list1(expr) ~ ")"
        },
        |(_, values, _)| values,
    );
    map(
        rule! {
           (VALUES ~ #value?)? ~ FROM ~ #at_string ~ #file_format_clause
        },
        |(values, _, location, format_options)| InsertSource::LoadFile {
            value: values.map(|(_, value)| value).unwrap_or_default(),
            location,
            format_options,
        },
    )
    .parse(i)
}

pub(crate) fn insert_source_fast_values(i: Input) -> IResult<InsertSource> {
    let values = map(
        rule! {
            VALUES ~ #rest_str
        },
        |(_, (rest_str, start))| InsertSource::RawValues { rest_str, start },
    );
    let query = map(
        rule! {
            #query
        },
        |query| InsertSource::Select {
            query: Box::new(query),
        },
    );

    rule!(
        #insert_source_file
        | #values
        | #query
    )
    .parse(i)
}

pub(crate) fn mutation_source(i: Input) -> IResult<MutationSource> {
    let query_source = map(rule! {#query ~ #table_alias}, |(query, source_alias)| {
        MutationSource::Select {
            query: Box::new(query),
            source_alias,
        }
    });

    let source_table = map(
        rule!(#dot_separated_idents_1_to_3 ~ #with_options? ~ #table_alias?),
        |((database, schema, table), with_options, alias)| MutationSource::Table {
            database,
            schema,
            table,
            with_options,
            alias,
        },
    );

    rule!(
        #query_source
        | #source_table
    )
    .parse(i)
}

fn when_clause(i: Input) -> IResult<WhenClause> {
    map(
        rule! {
            WHEN ~ ^#expr ~ THEN ~ (#into_clause)+
        },
        |(_, expr, _, into_clauses)| WhenClause {
            condition: expr,
            into_clauses,
        },
    )
    .parse(i)
}

fn into_clause(i: Input) -> IResult<IntoClause> {
    let source_expr = alt((
        map(rule! {DEFAULT}, |_| SourceExpr::Default),
        map(rule! { #expr }, SourceExpr::Expr),
    ));
    map(
        rule! {
            INTO
            ~ #dot_separated_idents_1_to_3
            ~ ( "(" ~ #comma_separated_list1(ident) ~ ")" )?
            ~ (VALUES ~ "(" ~ #comma_separated_list1(source_expr) ~ ")" )?
        },
        |(_, (database, schema, table), opt_target_columns, opt_source_columns)| IntoClause {
            database,
            schema,
            table,
            target_columns: opt_target_columns
                .map(|(_, columns, _)| columns)
                .unwrap_or_default(),
            source_columns: opt_source_columns
                .map(|(_, _, columns, _)| columns)
                .unwrap_or_default(),
        },
    )
    .parse(i)
}

fn else_clause(i: Input) -> IResult<ElseClause> {
    map(
        rule! {
            ELSE ~ (#into_clause)+
        },
        |(_, into_clauses)| ElseClause { into_clauses },
    )
    .parse(i)
}

pub(crate) fn match_clause(i: Input) -> IResult<MergeOption> {
    map(
        rule! {
            WHEN ~ MATCHED ~ (AND ~ ^#expr)? ~ THEN ~ #match_operation
        },
        |(_, _, expr_op, _, match_operation)| match expr_op {
            Some(expr) => MergeOption::Match(MatchedClause {
                selection: Some(expr.1),
                operation: match_operation,
            }),
            None => MergeOption::Match(MatchedClause {
                selection: None,
                operation: match_operation,
            }),
        },
    )
    .parse(i)
}

fn match_operation(i: Input) -> IResult<MatchOperation> {
    alt((
        value(MatchOperation::Delete, rule! { DELETE }),
        map(
            rule! {
                UPDATE ~ SET ~ ^#comma_separated_list1(mutation_update_expr)
            },
            |(_, _, update_list)| MatchOperation::Update {
                update_list,
                is_star: false,
            },
        ),
        map(
            rule! {
                UPDATE ~ "*"
            },
            |(_, _)| MatchOperation::Update {
                update_list: Vec::new(),
                is_star: true,
            },
        ),
    ))
    .parse(i)
}

pub(crate) fn unmatch_clause(i: Input) -> IResult<MergeOption> {
    alt((
        map(
            rule! {
                WHEN ~ NOT ~ MATCHED ~ (AND ~ ^#expr)? ~ THEN ~ INSERT ~ ( "(" ~ ^#comma_separated_list1(ident) ~ ^")" )?
                ~ VALUES ~ ^#row_values
            },
            |(_, _, _, expr_op, _, _, columns_op, _, values)| {
                let selection = expr_op.map(|e| e.1);
                match columns_op {
                    Some(columns) => MergeOption::Unmatch(UnmatchedClause {
                        insert_operation: InsertOperation {
                            columns: Some(columns.1),
                            values,
                            is_star: false,
                        },
                        selection,
                    }),
                    None => MergeOption::Unmatch(UnmatchedClause {
                        insert_operation: InsertOperation {
                            columns: None,
                            values,
                            is_star: false,
                        },
                        selection,
                    }),
                }
            },
        ),
        map(
            rule! {
                WHEN ~ NOT ~ MATCHED ~ (AND ~ ^#expr)? ~ THEN ~ INSERT ~ "*"
            },
            |(_, _, _, expr_op, _, _, _)| {
                let selection = expr_op.map(|e| e.1);
                MergeOption::Unmatch(UnmatchedClause {
                    insert_operation: InsertOperation {
                        columns: None,
                        values: vec![],
                        is_star: true,
                    },
                    selection,
                })
            },
        ),
    ))
    .parse(i)
}

pub(crate) fn mutation_update_expr(i: Input) -> IResult<MutationUpdateExpr> {
    map(
        rule! { #dot_separated_idents_1_to_2 ~ "=" ~ ^#expr },
        |((table, name), _, expr)| MutationUpdateExpr { table, name, expr },
    )
    .parse(i)
}
