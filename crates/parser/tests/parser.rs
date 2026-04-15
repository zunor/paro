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

use goldenfile::Mint;
use paro_parser::parser_testing::entry::parse_insert_partial;
use paro_parser::parser_testing::statement::statement_body;
use paro_parser::{parse_one_tokens, tokenize_sql};

#[path = "support/common.rs"]
mod common;

use common::{run_parser, GOLDEN_ROOT};

#[test]
fn test_insert_stmt_parse() {
    let cases = &[
        r#"insert into t (c1, c2) values (1, 2), (3, 4)"#,
        r#"insert into t (c1, c2) values (1, 2)"#,
        r#"insert into table t select * from t2"#,
        r#"insert into t (id, v) values (1, 2) on conflict (id) do nothing"#,
        r#"insert into t (id, v) values (1, 2) on conflict (id) do update set v = excluded.v"#,
    ];

    for case in cases {
        let tokens = tokenize_sql(case).unwrap();
        let stmt = parse_insert_partial(&tokens);
        assert!(stmt.is_ok(), "raw insert should parse: {}", case);
    }
}

#[test]
fn test_insert_on_conflict_stmt_display() {
    let sql =
        r#"insert into t (id, v) values (1, 2) on conflict (id) do update set v = excluded.v"#;
    let tokens = tokenize_sql(sql).unwrap();
    let stmt = parse_one_tokens(&tokens).unwrap();
    assert_eq!(
        stmt.stmt.to_string(),
        "INSERT INTO t (id, v) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET v = excluded.v"
    );
}

#[test]
fn test_reserved_error() {
    let mut mint = Mint::new(GOLDEN_ROOT);
    let file = &mut mint.new_goldenfile("reserved-error.txt").unwrap();
    let cases = &[r#"CREATE OR
            REPLACE FUNCTION ai_list_files (data_stage STAGE_LOCATION, l INT) RETURNS TABLE (
            stage VARCHAR,
            relative_path VARCHAR,
            path VARCHAR,
            is_dir BOOLEAN,
            size BIGINT,
            mode VARCHAR,
            content_type VARCHAR,
            etag VARCHAR,
            truncated BOOLEAN
           ) LANGUAGE python HANDLER='ai_list_files' address='https://api.bendml.com'"#];

    for case in cases {
        run_parser(file, statement_body, case);
    }
}
