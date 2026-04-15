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
