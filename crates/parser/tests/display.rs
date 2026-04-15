// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use paro_parser::parse_one;

fn test_stmt_display(sql: &str) {
    let stmt = parse_one(sql).unwrap();
    let sql1 = stmt.stmt.to_string();
    let stmt1 = parse_one(&sql1).unwrap();
    let sql2 = stmt1.stmt.to_string();
    assert_eq!(sql1, sql2);
}

#[test]
fn test_multi_table_insert_display() {
    const SQL_FILE_PATH: &str = "tests/fixtures/sql/multi_table_insert.sql";
    let sqls = std::fs::read_to_string(SQL_FILE_PATH).unwrap();
    for sql in sqls.split(';').filter(|s| !s.is_empty()) {
        test_stmt_display(sql.trim());
    }
}

#[test]
fn test_multi_table_insert_parse_error() {
    const SQL_FILE_PATH: &str = "tests/fixtures/sql/multi_table_insert_error.sql";
    let sqls = std::fs::read_to_string(SQL_FILE_PATH).unwrap();
    for sql in sqls.split(';').filter(|s| !s.is_empty()) {
        assert!(parse_one(sql.trim()).is_err());
    }
}
