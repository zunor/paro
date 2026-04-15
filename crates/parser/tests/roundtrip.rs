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

use paro_parser::{parse, parse_expr, parse_one};

fn assert_statement_roundtrip(sql: &str) {
    let parsed = parse_one(sql).expect("statement should parse");
    let canonical = parsed.stmt.to_string();
    let reparsed = parse_one(&canonical).expect("canonical statement should reparse");
    assert_eq!(reparsed.stmt.to_string(), canonical);
}

fn assert_expr_roundtrip(sql: &str) {
    let expr = parse_expr(sql).expect("expression should parse");
    let canonical = expr.to_string();
    let reparsed = parse_expr(&canonical).expect("canonical expression should reparse");
    assert_eq!(reparsed.to_string(), canonical);
}

fn assert_multi_statement_roundtrip(sql: &str) {
    let parsed = parse(sql).expect("statement list should parse");
    let canonical = parsed
        .iter()
        .map(|stmt| stmt.stmt.to_string())
        .collect::<Vec<_>>()
        .join("; ");
    let reparsed = parse(&canonical).expect("canonical statement list should reparse");
    let reparsed_canonical = reparsed
        .iter()
        .map(|stmt| stmt.stmt.to_string())
        .collect::<Vec<_>>()
        .join("; ");
    assert_eq!(reparsed_canonical, canonical);
}

#[test]
fn test_statement_roundtrip() {
    let cases = &[
        r#"WITH t AS (SELECT 1 AS a) SELECT * FROM t WHERE a > 0"#,
        r#"CREATE TABLE IF NOT EXISTS a.b (c INTEGER NOT NULL DEFAULT 1, b VARCHAR)"#,
        r#"ALTER TABLE t ADD COLUMN a FLOAT DEFAULT 1.1 COMMENT 'hello' FIRST"#,
        r#"CREATE VIEW v1(c1) AS SELECT number % 3 AS a FROM numbers(1000)"#,
        r#"CREATE INDEX idx_orders_customer_id ON orders (customer_id)"#,
        r#"CREATE UNIQUE INDEX idx_orders_customer_id ON orders (customer_id)"#,
        r#"INSERT INTO demo (id, name) VALUES (1, 'alice')"#,
        r#"SHOW GRANTS"#,
        r#"GRANT SELECT ON db01.tb1 TO ROLE role1"#,
        r#"SET SECONDARY ROLES ALL"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file EXPIRE = 7200 CONTENT_TYPE = 'application/octet-stream'"#,
        r#"EXPLAIN ANALYZE SELECT * FROM customer ORDER BY c_custkey LIMIT 5"#,
    ];

    for case in cases {
        assert_statement_roundtrip(case);
    }
}

#[test]
fn test_expr_roundtrip() {
    let cases = &[
        r#"a + b * c"#,
        r#"COUNT(DISTINCT x)"#,
        r#"SUM(x) FILTER (WHERE y > 0)"#,
        r#"LISTAGG(salary, '|') WITHIN GROUP (ORDER BY salary DESC NULLS LAST)"#,
        r#"ARRAY_FILTER(col, y -> y % 2 = 0)"#,
        r#"cosine_distance(embedding, [1, 2, 3]::Vector(3))"#,
    ];

    for case in cases {
        assert_expr_roundtrip(case);
    }
}

#[test]
fn test_multi_statement_roundtrip() {
    let case = r#"
        BEGIN;
        SET ROLE ROLE1;
        USE WAREHOUSE my_wh;
        COMMIT;
    "#;

    assert_multi_statement_roundtrip(case);
}
