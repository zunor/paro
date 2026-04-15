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

use paro_parser::ast::{Statement, TransactionKind};
use paro_parser::parse;
#[cfg(debug_assertions)]
use paro_parser::parser_testing::{parser_stack_stats_snapshot, reset_parser_stack_stats};

#[test]
fn test_multi_statements() {
    let sql = "SELECT 1; SELECT 2; SELECT 3";
    let stmts = parse(sql).unwrap();
    assert_eq!(stmts.len(), 3);
}

#[test]
fn test_multi_statements_with_semicolons() {
    let sql = "SELECT 1;;; SELECT 2;";
    let stmts = parse(sql).unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn test_multi_statements_with_format() {
    let sql = "SELECT 1 FORMAT JSON; SELECT 2";
    let stmts = parse(sql).unwrap();
    assert_eq!(stmts.len(), 2);
    let s1 = &stmts[0];
    assert_eq!(s1.format.as_deref(), Some("JSON"));
    let s2 = &stmts[1];
    assert!(s2.format.is_none());
}

#[test]
fn test_explain_statement_keeps_outer_format() {
    let sql = "EXPLAIN SELECT 1 FORMAT JSON";
    let stmts = parse(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    let s1 = &stmts[0];
    assert_eq!(s1.format.as_deref(), Some("JSON"));
    assert!(matches!(
        &s1.stmt,
        paro_parser::ast::Statement::Explain { .. }
    ));
}

#[test]
fn test_multi_statements_with_dollar_quote() {
    // Current CREATE PROCEDURE syntax requires RETURNS and LANGUAGE SQL
    let sql =
        "CREATE PROCEDURE foo() RETURNS INT LANGUAGE SQL AS $$ BEGIN SELECT 1; END; $$; SELECT 2;";
    let stmts = parse(sql).unwrap();
    assert_eq!(stmts.len(), 2);
    let s1 = &stmts[0].stmt;
    assert!(matches!(
        s1,
        paro_parser::ast::Statement::CreateProcedure(..)
    ));
    let s2 = &stmts[1].stmt;
    assert!(matches!(s2, paro_parser::ast::Statement::Query(..)));
}

#[test]
fn test_multi_statements_with_insert() {
    let sql = "INSERT INTO t VALUES (1); SELECT 2";
    let stmts = parse(sql).unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn test_multi_statements_error_quality() {
    let sql = "SELECT 1;\nCREATE TABEL t (i int);\nSELECT 3";
    let err = parse(sql).unwrap_err();
    let err_msg = err.to_string();
    println!("Captured Error Message:\n{}", err_msg);

    // Check if the error message is "excellent"
    assert!(err_msg.contains("unexpected `TABEL`"));
    assert!(err_msg.contains("expecting"));
    assert!(err_msg.contains("CREATE")); // Should show context
}

#[test]
fn test_transaction_savepoint_variants_parse_before_plain_rollback() {
    let stmts = parse(
        "ROLLBACK TO SAVEPOINT sp1; ROLLBACK TO sp2; RELEASE SAVEPOINT sp3; RELEASE sp4; COMMIT PREPARED 'gid'; ROLLBACK PREPARED 'gid2'",
    )
    .unwrap();

    assert_eq!(stmts.len(), 6);
    assert!(matches!(
        &stmts[0].stmt,
        Statement::Transaction(stmt)
            if matches!(&stmt.kind, TransactionKind::RollbackToSavepoint(name) if name.name == "sp1")
    ));
    assert!(matches!(
        &stmts[1].stmt,
        Statement::Transaction(stmt)
            if matches!(&stmt.kind, TransactionKind::RollbackToSavepoint(name) if name.name == "sp2")
    ));
    assert!(matches!(
        &stmts[2].stmt,
        Statement::Transaction(stmt)
            if matches!(&stmt.kind, TransactionKind::ReleaseSavepoint(name) if name.name == "sp3")
    ));
    assert!(matches!(
        &stmts[3].stmt,
        Statement::Transaction(stmt)
            if matches!(&stmt.kind, TransactionKind::ReleaseSavepoint(name) if name.name == "sp4")
    ));
    assert!(matches!(
        &stmts[4].stmt,
        Statement::Transaction(stmt)
            if matches!(&stmt.kind, TransactionKind::CommitPrepared(gid) if gid == "gid")
    ));
    assert!(matches!(
        &stmts[5].stmt,
        Statement::Transaction(stmt)
            if matches!(&stmt.kind, TransactionKind::RollbackPrepared(gid) if gid == "gid2")
    ));
}

#[test]
fn test_deep_multi_statements_keep_stack_guarded() {
    let sql = std::iter::repeat_n("SELECT (SELECT 1)", 256)
        .collect::<Vec<_>>()
        .join("; ");

    #[cfg(debug_assertions)]
    reset_parser_stack_stats();

    let stmts = parse(&sql).expect("deep multi-statement parse");
    assert_eq!(stmts.len(), 256);

    #[cfg(debug_assertions)]
    {
        let stats = parser_stack_stats_snapshot();
        assert!(
            stats.samples > 0,
            "expected stack samples for deep multi-statements"
        );
    }
}
